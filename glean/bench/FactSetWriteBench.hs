{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module FactSetWriteBench
where

import Control.Monad
import Data.Word (Word32)

import Criterion.Types

import Glean.Database.Test
import qualified Glean.RTS.Foreign.FactSet as FactSet
import Glean.Util.Benchmark

import RandomFacts

data B = B
  { bTag :: String
  , bSeed :: Word32
  , bRepeatFreq :: Double
  , bPreds :: [(String, RandomParams)]
}

benchs :: [B]
benchs =
  [B "src.File-10000" 4768 0.0 [("src.File", RandomParams 10000 73)]]

main :: IO ()
main = benchmarkMain $ \run ->
  withTestEnv [] $ \env -> do
    cases <- forM benchs $ \b@B{..} ->
      (,) b <$> randomFacts env (Just bSeed) bPreds
    run
      [ bench bTag
          $ whnfIO
          $ FactSet.copyWithRandomRepeats (Just $ bSeed + 1) bRepeatFreq facts
          | (B{..}, facts) <- cases ]