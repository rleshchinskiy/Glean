{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE ApplicativeDo #-}
module FactSetCopy (main) where

import Control.Monad (void)
import Options.Applicative

import Glean (Repo, parseRepo)
import qualified Glean.Database.Config as Database (Config, options)
import Glean.Database.Env (withDatabases)
import Glean.Database.Open (readDatabase)
import Glean.Impl.ConfigProvider
import qualified Glean.RTS.Foreign.FactSet as FactSet
import Glean.Util.ConfigProvider
import Util.EventBase (withEventBaseDataplane)
import Util.Timing

data Config = Config
  { configOptions :: Database.Config
  , configRepo :: Repo
  }

options :: ParserInfo Config
options = info (parser <**> helper)
  (fullDesc <> progDesc "Generate a random batch of facts")
  where
    parser = do
      configOptions <- Database.options
      configRepo <- option (maybeReader Glean.parseRepo)
        (  long "db"
        <> metavar "NAME/INSTANCE"
        <> help "identifies the database"
        )
      return Config{..}

main :: IO ()
main =
  withConfigOptions options $ \(Config{..}, cfg) ->
  withEventBaseDataplane $ \evb ->
  withConfigProvider cfg $ \(cfgAPI :: ConfigAPI) ->
  withDatabases evb configOptions cfgAPI $ \env -> do
    facts1 <- readDatabase env configRepo $ const bench
    void $ bench facts1
  where
    bench look = do
      (time, allocs, facts) <- timeIt $ FactSet.cloneContiguous look
      count <- FactSet.factCount facts
      mem <- FactSet.allocatedMemory facts
      factmem <- FactSet.factMemory facts
      putStrLn $ unwords
        [ "time:", showTime time
        , "facts:", show count
        , "mem:", showAllocs $ fromIntegral mem
        , "factmem:", showAllocs $ fromIntegral factmem
        , "allocs:", showAllocs allocs
        ]
      return facts
  