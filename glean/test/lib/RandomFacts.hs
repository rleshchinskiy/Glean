{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module RandomFacts (randomFacts, RandomParams(..)) where

import Control.Monad
import qualified Data.Text as Text
import Data.Word (Word32)
import System.Exit (die)

import Glean.Angle.Types (SourceRef(..))
import Glean.Database.Schema
import Glean.Database.Schema.Types
import Glean.Database.Types (Env(..))
import Glean.RTS.Foreign.FactSet (FactSet, RandomParams(..))
import qualified Glean.RTS.Foreign.FactSet as FactSet
import Glean.RTS.Types (Fid(..))
import Glean.RTS.Typecheck (randomFact)
import qualified Glean.Util.Observed as Observed

randomFacts
  :: Env -> Maybe Word32 -> [(String, RandomParams)] -> IO FactSet
randomFacts env seed specs = do
  schemas <- Observed.get $ envSchemaSource env
  db_schema <- newDbSchema schemas LatestSchemaAll readWriteContent

  preds <- forM specs $ \(name, params) ->
    case lookupPredicateSourceRef
          SourceRef{sourceRefName = Text.pack name, sourceRefVersion = Nothing}
          LatestSchemaAll
          db_schema of
      Right PredicateDetails{..} -> do
        gen <- randomFact predicateKeyType predicateValueType
        return (predicatePid, params, gen)
      
      Left err -> die $ Text.unpack err

  FactSet.random (Fid 1024) seed preds
