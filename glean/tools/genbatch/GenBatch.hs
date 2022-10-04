{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE ApplicativeDo #-}
module GenBatch (main) where

import Control.Monad
import qualified Data.ByteString as ByteString
import qualified Data.Text as Text
import Data.Proxy (Proxy(..))
import Data.Word (Word32)
import Options.Applicative
import System.Exit (die)
import Thrift.Codegen (serializeGen)
import Thrift.Protocol.Compact (Compact)

import Glean (ThriftSource)
import Glean.Angle.Types (SourceRef(..))
import Glean.Database.Config (schemaSourceOption, SchemaIndex)
import Glean.Database.Schema
import Glean.Database.Schema.Types (lookupPredicateSourceRef, SchemaSelector (LatestSchemaAll))
import Glean.Impl.ConfigProvider
import qualified Glean.RTS.Foreign.FactSet as FactSet
import Glean.RTS.Types (Fid(..))
import Glean.Util.ConfigProvider
import qualified Glean.Util.ThriftSource as ThriftSource
import Glean.RTS.Typecheck (randomFact)

data Config = Config
  { configSchemaSource :: ThriftSource SchemaIndex
  , configSchemaPath :: Maybe FilePath
  , configOutput :: FilePath
  , configFirstId :: Fid
  , configSeed :: Maybe Word32
  , configPredicates :: [(String, FactSet.RandomParams)]
  }

options :: ParserInfo Config
options = info (parser <**> helper)
  (fullDesc <> progDesc "Generate a random batch of facts")
  where
    parser = do
      ~(configSchemaPath, configSchemaSource) <- schemaSourceOption
      configOutput <- strOption
        ( short 'o'
        <> long "output"
        <> metavar "PATH" )
      configFirstId <- Fid <$> option auto
        ( long "first-id"
        <> value 1024
        <> metavar "N" )
      configSeed <- optional $ option auto
        ( long "seed"
        <> metavar "N" )
      configPredicates <- many $ argument predicate
        ( metavar "NAME,COUNT[,SIZES]")
      return Config{..}
      where
        predicate = maybeReader $ \s -> do
          (pred, ',':t) <- pure $ break (==',') s
          (sz,r) <- do
            let (sz,r) = break (==',') t
            (,) sz <$> case r of
              ',':p -> num p
              _ -> pure 71
          n <- num sz
          return (pred, FactSet.RandomParams n r)
        
        num s = case reads s of
          [(n,"")] -> Just (n::Int)
          _ -> Nothing


main :: IO ()
main =
  withConfigOptions options $ \(Config{..}, cfg) ->
  withConfigProvider cfg $ \(cfgAPI :: ConfigAPI) -> do

  schemas <- ThriftSource.load cfgAPI configSchemaSource
  db_schema <- newDbSchema schemas LatestSchemaAll readWriteContent

  preds <- forM configPredicates $ \(name, params) ->
    case lookupPredicateSourceRef
          SourceRef{sourceRefName = Text.pack name, sourceRefVersion = Nothing}
          LatestSchemaAll
          db_schema of
      Right PredicateDetails{..} -> do
        gen <- randomFact predicateKeyType predicateValueType
        return (predicatePid, params, gen)
      
      Left err -> die $ Text.unpack err

  facts <- FactSet.random configFirstId configSeed preds
  batch <- FactSet.serialize facts
  ByteString.writeFile configOutput
    $ serializeGen (Proxy :: Proxy Compact) batch
  