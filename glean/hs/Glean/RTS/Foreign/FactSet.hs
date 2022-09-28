{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.RTS.Foreign.FactSet
  ( FactSet
  , new
  , factCount
  , factMemory
  , allocatedMemory
  , predicateStats
  , firstFreeId
  , serialize
  , serializeReorder
  , append
  , rebase
  , renameFacts

  , cloneContiguous

  , RandomParams(..)
  , random
  , copyWithRandomRepeats

  , copyFactBlock
  , fromFactBlock

  , containsById
  , containsByKey
  ) where

import Control.Exception
import Data.Coerce (coerce)
import Data.Int
import qualified Data.Vector.Storable as V
import Data.Word
import Foreign.C.String
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Array (withArray)
import Foreign.Ptr

import Util.FFI

import Glean.FFI
import Glean.RTS.Foreign.Bytecode (Subroutine)
import Glean.RTS.Foreign.Define
import Glean.RTS.Foreign.Inventory (Inventory)
import Glean.RTS.Foreign.Lookup (Lookup(..), CanLookup(..))
import Glean.RTS.Foreign.LookupCache (LookupCache)
import Glean.RTS.Foreign.Stacked (stacked)
import Glean.RTS.Foreign.Stats (marshalPredicateStats)
import Glean.RTS.Foreign.Subst (Subst)
import Glean.RTS.Types (Fid(..), Pid(..))
import qualified Glean.Types as Thrift

-- An environment for defining facts
newtype FactSet = FactSet (ForeignPtr FactSet)

instance Object FactSet where
  wrap = FactSet
  unwrap (FactSet p) = p
  destroy = glean_factset_free

instance CanLookup FactSet where
  lookupName _ = "factset"
  withLookup x f = with x $ f . glean_factset_lookup

instance CanDefine FactSet where
  withDefine x f = with x $ f . glean_factset_define

new :: Fid -> IO FactSet
new next_id = construct $ invoke $ glean_factset_new next_id

factCount :: FactSet -> IO Word64
factCount facts = fromIntegral <$> with facts glean_factset_fact_count

factMemory :: FactSet -> IO Word64
factMemory facts = fromIntegral <$> with facts glean_factset_fact_memory

allocatedMemory :: FactSet -> IO Word64
allocatedMemory facts =
  fromIntegral <$> with facts glean_factset_allocated_memory

predicateStats :: FactSet -> IO [(Pid, Thrift.PredicateStats)]
predicateStats facts = with facts
    $ marshalPredicateStats . glean_factset_predicateStats

firstFreeId :: FactSet -> IO Fid
firstFreeId facts = with facts glean_factset_first_free_id

mkBatch :: IO (Fid, CSize, Ptr (), CSize) -> IO Thrift.Batch
mkBatch fn = mask_ $ do
  (Fid first_id, count, facts_data, facts_size) <- fn
  Thrift.Batch first_id (fromIntegral count)
    <$> unsafeMallocedByteString facts_data facts_size
    <*> pure Nothing
    <*> pure mempty

serialize :: FactSet -> IO Thrift.Batch
serialize facts =
  with facts $ \facts_ptr -> do
  mkBatch $ invoke $ glean_factset_serialize facts_ptr

serializeReorder :: FactSet -> V.Vector Int64 -> IO Thrift.Batch
serializeReorder facts order =
  with facts $ \facts_ptr -> do
  V.unsafeWith order $ \order_ptr -> do
    mkBatch $ invoke $ glean_factset_serializeReorder
      facts_ptr
      order_ptr
      (fromIntegral (V.length order))

rebase :: Inventory -> Thrift.Subst -> LookupCache -> FactSet -> IO FactSet
rebase inventory Thrift.Subst{..} cache facts =
  with inventory $ \inventory_ptr ->
  V.unsafeWith subst_ids $ \ids_ptr ->
  with cache $ \cache_ptr ->
  with facts $ \facts_ptr ->
  construct $ invoke $
    glean_factset_rebase
      facts_ptr
      inventory_ptr
      (Fid subst_firstId)
      (fromIntegral $ V.length subst_ids)
      ids_ptr
      cache_ptr

append :: FactSet -> FactSet -> IO ()
append target source =
  with target $ \target_ptr ->
  with source $ \source_ptr ->
  invoke $ glean_factset_append target_ptr source_ptr

-- Prepare a Thrift batch for writing into the database by renaming and
-- deduplicating facts.
renameFacts
  :: CanLookup l
  => Inventory          -- ^ where to lookup predicates
  -> l                  -- ^ where to lookup facts
  -> Fid                -- ^ first free fact id in the database
  -> Thrift.Batch       -- ^ batch to rename
  -> IO (FactSet, Subst)
                        -- ^ resulting facts and substitution
renameFacts inventory base next batch = do
  added <- new next
  subst <- defineUntrustedBatch (stacked base added) inventory batch
  return (added, subst)

cloneContiguous :: CanLookup l => l -> IO FactSet
cloneContiguous look = withLookup look
  $ construct . invoke . glean_factset_clone_contiguous


data RandomParams = RandomParams
  { randomParamsWanted :: Int
  , randomParamsSizeCap :: Int
  }

random
  :: Fid
  -> Maybe Word32
  -> [(Pid, RandomParams, Subroutine ())]
  -> IO FactSet
random firstId seed xs =
  withArray preds $ \preds_ptr ->
  withArray (map (fromIntegral . randomParamsWanted) params) $ \wanted_ptr ->
  withArray (map (fromIntegral . randomParamsSizeCap) params) $ \sizes_ptr ->
  withMany with gens $ \gen_ps ->
  withArray gen_ps $ \gens_ptr ->
  construct $ invoke $ glean_factset_random
    firstId
    (maybe (-1) fromIntegral seed)
    (fromIntegral n)
    preds_ptr
    wanted_ptr
    sizes_ptr
    gens_ptr
  where
    (preds, params, gens) = unzip3 xs
    !n = length xs

copyWithRandomRepeats
  :: Maybe Word32
  -> Double
  -> FactSet
  -> IO FactSet
copyWithRandomRepeats seed repeatFreq facts =
  with facts $ \facts_ptr ->
  construct $ invoke $ glean_factset_copy_with_random_repeats
    (maybe (-1) fromIntegral seed)
    (coerce repeatFreq)
    facts_ptr

newtype FactBlock = FactBlock (ForeignPtr FactBlock)

instance Object FactBlock where
  wrap = FactBlock
  unwrap (FactBlock p) = p
  destroy = glean_factblock_free

copyFactBlock :: CanLookup l => l -> IO FactBlock
copyFactBlock l = withLookup  l $ \look ->
  construct $ invoke $ glean_factblock_copy look

fromFactBlock :: FactBlock -> IO FactSet
fromFactBlock b = with b $ \p ->
  construct $ invoke $ glean_factset_from_factblock p

containsById :: CanLookup l => l -> FactBlock -> IO Bool
containsById l block =
  withLookup l $ \look ->
  with block $ \ptr -> do
    r <- invoke $ glean_lookup_contains_by_id look ptr
    return $ r /= 0

containsByKey ::  CanLookup l => l -> FactBlock -> IO Bool
containsByKey l block =
  withLookup l $ \look ->
  with block $ \ptr -> do
    r <- invoke $ glean_lookup_contains_by_key look ptr
    return $ r /= 0

foreign import ccall unsafe glean_factset_new
  :: Fid -> Ptr (Ptr FactSet) -> IO CString
foreign import ccall unsafe "&glean_factset_free" glean_factset_free
  :: FunPtr (Ptr FactSet -> IO ())

foreign import ccall unsafe glean_factset_fact_count
  :: Ptr FactSet -> IO CSize

foreign import ccall unsafe glean_factset_fact_memory
  :: Ptr FactSet -> IO CSize

foreign import ccall unsafe glean_factset_allocated_memory
  :: Ptr FactSet -> IO CSize

foreign import ccall safe glean_factset_predicateStats
  :: Ptr FactSet
  -> Ptr CSize
  -> Ptr (Ptr Int64)
  -> Ptr (Ptr Word64)
  -> Ptr (Ptr Word64)
  -> IO CString

foreign import ccall unsafe glean_factset_first_free_id
  :: Ptr FactSet -> IO Fid

foreign import ccall unsafe glean_factset_lookup
  :: Ptr FactSet -> Ptr Lookup

foreign import ccall unsafe glean_factset_define
  :: Ptr FactSet -> Define

foreign import ccall unsafe glean_factset_serialize
  :: Ptr FactSet
  -> Ptr Fid
  -> Ptr CSize
  -> Ptr (Ptr ())
  -> Ptr CSize
  -> IO CString

foreign import ccall unsafe glean_factset_serializeReorder
  :: Ptr FactSet
  -> Ptr Int64
  -> CSize
  -> Ptr Fid
  -> Ptr CSize
  -> Ptr (Ptr ())
  -> Ptr CSize
  -> IO CString

foreign import ccall unsafe glean_factset_rebase
  :: Ptr FactSet
  -> Ptr Inventory
  -> Fid
  -> CSize
  -> Ptr Int64
  -> Ptr LookupCache
  -> Ptr (Ptr FactSet)
  -> IO CString

foreign import ccall unsafe glean_factset_append
  :: Ptr FactSet -> Ptr FactSet -> IO CString

foreign import ccall safe glean_factset_clone_contiguous
  :: Ptr Lookup
  -> Ptr (Ptr FactSet)
  -> IO CString

foreign import ccall safe glean_factset_random
  :: Fid
  -> Int64
  -> CSize
  -> Ptr Pid
  -> Ptr CSize
  -> Ptr CSize
  -> Ptr (Ptr (Subroutine ()))
  -> Ptr (Ptr FactSet)
  -> IO CString

foreign import ccall safe glean_factset_copy_with_random_repeats
  :: Int64
  -> CDouble
  -> Ptr FactSet
  -> Ptr (Ptr FactSet)
  -> IO CString

foreign import ccall safe glean_factblock_copy
  :: Ptr Lookup
  -> Ptr (Ptr FactBlock)
  -> IO CString

foreign import ccall unsafe "&glean_factblock_free" glean_factblock_free
  :: FunPtr (Ptr FactBlock -> IO ())

foreign import ccall safe glean_factset_from_factblock
  :: Ptr FactBlock
  -> Ptr (Ptr FactSet)
  -> IO CString


foreign import ccall safe glean_lookup_contains_by_id
  :: Ptr Lookup
  -> Ptr FactBlock
  -> Ptr CBool
  -> IO CString


foreign import ccall safe glean_lookup_contains_by_key
  :: Ptr Lookup
  -> Ptr FactBlock
  -> Ptr CBool
  -> IO CString