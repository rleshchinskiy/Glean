{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE ApplicativeDo #-}
module FactSetCopy (main) where

import Control.Monad
import Options.Applicative
import System.Exit (die)

import Glean (Repo, parseRepo)
import qualified Glean.Database.Config as Database (Config, options)
import Glean.Database.Env (withDatabases)
import Glean.Database.Open (readDatabase)
import Glean.Database.Types (Env)
import Glean.Impl.ConfigProvider
import qualified Glean.RTS.Foreign.FactSet as FactSet
import qualified Glean.RTS.Foreign.Lookup as Lookup
import Glean.Util.ConfigProvider
import Util.EventBase (withEventBaseDataplane)
import Util.Timing

type Benchmark = Config -> Env -> IO ()

data Config = Config
  { configOptions :: Database.Config
  , configRepo :: Repo
  , configBenchmark :: Benchmark
  , configSpin :: Bool
  }

benchmarks :: [(String, Benchmark)]
benchmarks =
  [ ("suite", suite)
  , ("spinFromFactBlock",  spinFromFactBlock)
  , ("spinById",  spinById)
  , ("spinByKey",  spinByKey)
  , ("seekEach", seekEach) ]

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
      configBenchmark <- argument (maybeReader $ \s -> lookup s benchmarks)
        ( metavar "BENCHMARK"
        <> value suite
        <> help "selects benchmark"
        )
      configSpin <- switch
        ( long "spin"
        <> help "spin forever ")
      return Config{..}

main :: IO ()
main =
  withConfigOptions options $ \(config@Config{..}, cfg) ->
  withEventBaseDataplane $ \evb ->
  withConfigProvider cfg $ \(cfgAPI :: ConfigAPI) ->
  withDatabases evb configOptions cfgAPI $ \env ->
    configBenchmark config env
    {- do
  block <- withDatabases evb configOptions cfgAPI $ \env -> do
    facts1 <- readDatabase env configRepo $ \_ -> bench "read" . FactSet.cloneContiguous
    facts2 <- bench "copy" $ FactSet.cloneContiguous facts1
    block <- bench3 "block" $ FactSet.copyFactBlock facts2
    -- readDatabase env configRepo $ const FactSet.copyFactBlock
    facts3 <- bench "from" $ FactSet.fromFactBlock block
    readDatabase env configRepo $ \_ db ->
      bench2 "byId (db)" $ FactSet.containsById db block
    bench2 "byId (fs)" $ FactSet.containsById facts3 block
    readDatabase env configRepo $ \_ db ->
      bench2 "byKey (db)" $ FactSet.containsByKey db block
    bench2 "byKey (fs)" $ FactSet.containsByKey facts3 block
    stats <- FactSet.predicateStats facts3
    readDatabase env configRepo $ \_ db ->
      bench2 "seek (db)" $ True <$ forM_ stats (Lookup.seekCount db . fst)
    bench2 "seek (fs)" $ True <$ forM_ stats (Lookup.seekCount facts3 . fst)
  return ()

  -- forever $ void $ FactSet.fromFactBlock block
  -}

suite :: Benchmark
suite Config{..} env = do
  facts1 <- readDatabase env configRepo $ \_ -> bench "read" . FactSet.cloneContiguous
  facts2 <- bench "copy" $ FactSet.cloneContiguous facts1
  block <- bench3 "block" $ FactSet.copyFactBlock facts2
  -- readDatabase env configRepo $ const FactSet.copyFactBlock
  facts3 <- bench "from" $ FactSet.fromFactBlock block
  readDatabase env configRepo $ \_ db ->
    bench2 "byId (db)" $ FactSet.containsById db block
  bench2 "byId (fs)" $ FactSet.containsById facts3 block
  readDatabase env configRepo $ \_ db ->
    bench2 "byKey (db)" $ FactSet.containsByKey db block
  bench2 "byKey (fs)" $ FactSet.containsByKey facts3 block
  readDatabase env configRepo $ \_ db->
    bench2 "seekEeach (db)" $ FactSet.seekEach db block
  bench2 "seekEeach (fs)" $ FactSet.seekEach facts3 block
  stats <- FactSet.predicateStats facts3
  readDatabase env configRepo $ \_ db ->
    bench2 "seek (db)" $ True <$ forM_ stats (Lookup.seekCount db . fst)
  bench2 "seek (fs)" $ True <$ forM_ stats (Lookup.seekCount facts3 . fst)

getBlock :: Config -> Env -> IO FactSet.FactBlock
getBlock Config{..} env =
    readDatabase env configRepo $ const FactSet.copyFactBlock

spinFromFactBlock :: Benchmark
spinFromFactBlock config env = do
  block <- getBlock config env
  forever $ void $ FactSet.fromFactBlock block

spinById :: Benchmark
spinById config env = do
  block <- getBlock config env
  facts <- FactSet.fromFactBlock block
  forever $ void $ FactSet.containsById facts block

spinByKey :: Benchmark
spinByKey config env = do
  block <- getBlock config env
  facts <- FactSet.fromFactBlock block
  forever $ void $ FactSet.containsByKey facts block

seekEach :: Benchmark
seekEach config env = do
  block <- getBlock config env
  facts <- FactSet.fromFactBlock block
  let run
        | configSpin config = forever . void
        | otherwise = bench2 "seekEeach (fs)"
  run $ FactSet.seekEach facts block

bench :: String -> IO FactSet.FactSet -> IO FactSet.FactSet
bench tag action = do
  (time, allocs, facts) <- timeIt action
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

bench3 :: String -> IO a -> IO a
bench3 tag action = do
  (time, _, res) <- timeIt action
  putStrLn $ unwords [tag, showTime time]
  return res