{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.RTS.Foreign.Lookup
  ( Lookup(..)
  , CanLookup(..)
  , withCanLookup
  , EmptyLookup(..)
  , startingId
  , firstFreeId
  , lookupFact
  , Limits(..)
  , serialize
  , withSnapshot
  , containsById
  , containsByKey
  , seekCount
) where

import Control.Exception (bracket, mask_)
import Data.Coerce (coerce)
import Data.Default
import Data.Int
import Data.Text
import Data.Word (Word64)
import Foreign.C
import Foreign.Ptr

import Util.FFI

import Glean.FFI
import Glean.RTS.Types (Fid(..), invalidFid, Pid(..))
import qualified Glean.Types as Thrift

-- | A reference to a thing we can look up facts in
data Lookup = Lookup
  { lookupPtr :: Ptr Lookup
  , lookupName_ :: Text
  }

-- | Class of things we can look up facts in
class CanLookup a where
  withLookup :: a -> (Ptr Lookup -> IO b) -> IO b
  lookupName :: a -> Text

instance CanLookup Lookup where
  withLookup l f = f (lookupPtr l)
  lookupName = lookupName_

withCanLookup :: CanLookup lookup => lookup -> (Lookup -> IO a) -> IO a
withCanLookup lookup f =
  withLookup lookup $ \p -> f (Lookup p (lookupName lookup))

startingId :: CanLookup a => a -> IO Fid
startingId x = withLookup x $ \l -> invoke $ glean_lookup_starting_id l

firstFreeId :: CanLookup a => a -> IO Fid
firstFreeId x = withLookup x $ \l -> invoke $ glean_lookup_first_free_id l

data EmptyLookup = EmptyLookup

instance CanLookup EmptyLookup where
  lookupName EmptyLookup = "lookup:empty"
  withLookup EmptyLookup f =
    bracket
      (invoke glean_lookup_empty)
      glean_lookup_free
      f

lookupFact :: CanLookup a => a -> Fid -> IO (Maybe Thrift.Fact)
lookupFact look fid =
  withLookup look $ \look_ptr -> do
    (pid, key_ptr, key_size, val_ptr, val_size) <-
      invoke $ glean_lookup_fact look_ptr fid
    if pid == 0
      then return Nothing
      else fmap Just $ Thrift.Fact pid
            <$> unsafeMallocedByteString key_ptr key_size
            <*> unsafeMallocedByteString val_ptr val_size

data Limits = Limits
  { limitFrom :: Fid
  , limitUpto :: Fid
  , limitMaxFacts :: Word64
  , limitMaxBytes :: Word64
  }

instance Default Limits where
  def = Limits
    { limitFrom = invalidFid
    , limitUpto = invalidFid
    , limitMaxFacts = maxBound
    , limitMaxBytes = maxBound
    }

serialize :: CanLookup a => a -> Limits -> IO Thrift.Batch
serialize look Limits{..} = mask_ $
  withLookup look $ \look_ptr -> do
  (Fid first_id, count, facts, facts_size, ids, ids_size) <- invoke $
    glean_lookup_serialize
      look_ptr
      limitFrom
      limitUpto
      (fromIntegral limitMaxFacts)
      (fromIntegral limitMaxBytes)
  Thrift.Batch first_id (fromIntegral count)
    <$> unsafeMallocedByteString facts facts_size
    <*> (if ids /= nullPtr
          then Just <$> unsafeMallocedVector (coerce ids) ids_size
          else pure Nothing)
    <*> pure mempty

-- | Restrict the Lookup to facts up to the specified fact id
withSnapshot :: CanLookup a => a -> Fid -> (Lookup -> IO b) -> IO b
withSnapshot base boundary f =
  withLookup base $ \base_ptr ->
  bracket
    (invoke $ glean_snapshot_new base_ptr boundary)
    glean_lookup_free
    (\p -> f (Lookup p (lookupName base)))

containsById :: (CanLookup a, CanLookup b) => a -> b -> IO Bool
containsById big small =
  withLookup big $ \big_ptr ->
  withLookup small $ \small_ptr -> do
    r <- invoke $ glean_lookup_contains_by_id big_ptr small_ptr
    return $ r /= 0

containsByKey :: (CanLookup a, CanLookup b) => a -> b -> IO Bool
containsByKey big small =
  withLookup big $ \big_ptr ->
  withLookup small $ \small_ptr -> do
    r <- invoke $ glean_lookup_contains_by_key big_ptr small_ptr
    return $ r /= 0

seekCount :: CanLookup a => a -> Pid -> IO Int
seekCount look (Pid pid) =
  withLookup look $ \look_ptr ->
  fmap fromIntegral $ invoke $ glean_lookup_seek_count look_ptr pid

foreign import ccall unsafe glean_lookup_empty
  :: Ptr (Ptr Lookup) -> IO CString

foreign import ccall unsafe glean_lookup_free
  :: Ptr Lookup -> IO ()

foreign import ccall unsafe glean_snapshot_new
  :: Ptr Lookup -> Fid -> Ptr (Ptr Lookup) -> IO CString

foreign import ccall unsafe glean_lookup_starting_id
  :: Ptr Lookup -> Ptr Fid -> IO CString
foreign import ccall unsafe glean_lookup_first_free_id
  :: Ptr Lookup -> Ptr Fid -> IO CString

foreign import ccall safe glean_lookup_fact
  :: Ptr Lookup
  -> Fid
  -> Ptr Int64
  -> Ptr (Ptr ())
  -> Ptr CSize
  -> Ptr (Ptr ())
  -> Ptr CSize
  -> IO CString

foreign import ccall safe glean_lookup_serialize
  :: Ptr Lookup
  -> Fid
  -> Fid
  -> CSize
  -> CSize
  -> Ptr Fid
  -> Ptr CSize
  -> Ptr (Ptr ())
  -> Ptr CSize
  -> Ptr (Ptr Fid)
  -> Ptr CSize
  -> IO CString

foreign import ccall safe glean_lookup_contains_by_id
  :: Ptr Lookup
  -> Ptr Lookup
  -> Ptr CBool
  -> IO CString


foreign import ccall safe glean_lookup_contains_by_key
  :: Ptr Lookup
  -> Ptr Lookup
  -> Ptr CBool
  -> IO CString

foreign import ccall safe glean_lookup_seek_count
  :: Ptr Lookup
  -> Int64
  -> Ptr CSize
  -> IO CString