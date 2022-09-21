{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE ApplicativeDo #-}
module FactSetCopy (main) where

import Control.Monad (forM_, when)
import Options.Applicative
import System.Exit (die)

import Glean (Repo, parseRepo)
import qualified Glean.Database.Config as Database (Config, options)
import Glean.Database.Env (withDatabases)
import Glean.Database.Open (readDatabase)
import Glean.Impl.ConfigProvider
import qualified Glean.RTS.Foreign.FactSet as FactSet
import qualified Glean.RTS.Foreign.Lookup as Lookup
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
    facts1 <- readDatabase env configRepo $ const $ bench "read"
    facts2 <- bench "copy" facts1
    readDatabase env configRepo $ \_ db ->
      bench2 "byId (db)" $ Lookup.containsById db facts1
    bench2 "byId (fs)" $ Lookup.containsById facts2 facts1
    readDatabase env configRepo $ \_ db ->
      bench2 "byKey (db)" $ Lookup.containsByKey db facts1
    bench2 "byKey (fs)" $ Lookup.containsByKey facts2 facts1
    stats <- FactSet.predicateStats facts2
    readDatabase env configRepo $ \_ db ->
      bench2 "seek (db)" $ True <$ forM_ stats (Lookup.seekCount db . fst)
    bench2 "seek (fs)" $ True <$ forM_ stats (Lookup.seekCount facts2 . fst)
  where
    bench tag look = do
      (time, allocs, facts) <- timeIt $ FactSet.cloneContiguous look
      count <- FactSet.factCount facts
      mem <- FactSet.allocatedMemory facts
      factmem <- FactSet.factMemory facts
      putStrLn $ unwords
        [ tag
        , "time:", showTime time
        , "facts:", show count
        , "mem:", showAllocs $ fromIntegral mem
        , "factmem:", showAllocs $ fromIntegral factmem
        , "allocs:", showAllocs allocs
        ]
      return facts

    bench2 :: String -> IO Bool -> IO ()
    bench2 tag action = do
      (time, _, res) <- timeIt action
      when (not res) $ die $ tag ++ " failed"
      putStrLn $ unwords [tag, showTime time]