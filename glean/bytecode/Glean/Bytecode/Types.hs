{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.Bytecode.Types
  ( Ty(..)
  , Ordered
  , Addable
  , Subable(..)
  , Register(..)
  , Label(..)
  , Literal(..)
  , castRegister
  ) where

import Data.Coerce (coerce)
import Data.Word (Word64)

-- | Instruction argument types
data Ty
  = Word -- ^ 64 bit word
  | WordPtr -- ^ pointer to 64 bit word
  | Lit -- ^ index into the (string) literal table
  | Offset -- ^ jump offset (relative to start of next instruction)
  | DataPtr -- ^ a void pointer
  | BinaryOutputPtr -- ^ pointer to binary::Output (temporary, will be removed)
  deriving(Eq, Show)

-- | Types which can be compared
class Ordered (t :: Ty)
instance Ordered 'Word
instance Ordered 'DataPtr

-- | Types which can be added
class Addable (t :: Ty) (u :: Ty)
instance Addable 'Word 'Word
instance Addable 'DataPtr 'Word

class Subable (a :: Ty) (b :: Ty) where
  type Difference a b :: Ty

instance Subable 'Word 'Word where
  type Difference 'Word 'Word = 'Word
instance Subable 'DataPtr 'Word where
  type Difference 'DataPtr 'Word = 'DataPtr
instance Subable 'DataPtr 'DataPtr where
  type Difference 'DataPtr 'DataPtr = 'Word

-- | Typed registers
newtype Register (ty :: Ty) = Register { fromRegister :: Word64 }
  deriving(Eq,Ord,Enum,Show)

castRegister :: Register a -> Register b
castRegister = coerce

-- | Labels
newtype Label = Label { fromLabel :: Int }
  deriving(Eq,Ord,Enum,Show)

newtype Literal = Literal { fromLiteral :: Word64 }
  deriving(Eq,Ord,Show)
