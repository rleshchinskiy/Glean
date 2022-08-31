{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.Bytecode.Generate.Instruction
  ( Insn(..)
  , Effect(..)
  , Arg(..)
  , ArgTy(..)
  , ImmTy(..)
  , Usage(..)
  , instructions
  , version
  , lowestSupportedVersion
  , Opd(..)
  , insnOperands
  , insnUsedBits
  ) where

import Data.List (scanl')
import Data.Text (Text)

import Glean.Bytecode.Types

-- This is an instruction set for a really simple register machine. The
-- instructions themselves haven't been designed in any sense, we just added
-- whatever seemed necessary to implement typechecking via bytecode. The
-- instruction stream is a sequence of 64 bit words, with each instruction
-- having one word for the opcode and one word per argument. We will revisit
-- both the instruction set and the representation once this is actually used.

-- | Definition of a bytecode instruction.
data Insn = Insn
  { insnName :: Text
  , insnEffects :: [Effect]
  , insnContext :: [Text]
  , insnArgs :: [Arg]
  }

-- | Effects of an instruction
data Effect
  = EndBlock  -- ^ end the current block (typically for jumps)
  | Return    -- ^ return from the evaluator
  deriving(Eq, Show)

-- | Instruction argument.
data Arg = Arg
  { argName :: Text
  , argTy :: ArgTy
  }

-- | Argument type
data ArgTy
  = Imm ImmTy -- ^ immediate value in instruction stream
  | Reg (Maybe Text) Ty Usage -- ^ register argument
  | Offsets -- ^ array of jump offsets (length + array in the insn stream)
  | Regs -- ^ list of registers (array without length in the insn stream)

-- | Type of an immediate value
data ImmTy
  = U8
  | U32
  | I8
  | I32
  | ImmOffset
  | ImmLit
  deriving(Eq,Ord,Show)

-- | Register argument of a particular type
reg :: Ty -> Usage -> ArgTy
reg = Reg Nothing

-- | Polymorphic register argument - `Text` is the type variable and `Ty` the
-- type actually stored in the instruction (since we don't really need
-- existentials there)
polyReg :: Text -> Ty -> Usage -> ArgTy
polyReg = Reg . Just

-- | How an register is used
data Usage
  = Load -- ^ read-only
  | Store -- ^ write-only
  | Update -- ^ read and write
  deriving(Eq)

-- | Current bytecode version
--
-- BUMP THIS WHENEVER YOU CHANGE THE BYTECODE EVEN IF YOU JUST ADD INSTRUCTIONS
version :: Int
version = 7

-- | Lowest bytecode version supported by the current engine.
--
-- SET THIS TO THE SAME VALUE AS 'version' UNLESS YOU ONLY ADD NEW INSTRUCTIONS
-- TO THE END OF THE LIST (in which case the new engine can still execute
-- old bytecode)
lowestSupportedVersion :: Int
lowestSupportedVersion = 7

-- | Definitions of all bytecode instructions
instructions :: [Insn]
instructions =
  [
    -- Copy a register into another one.
    Insn "Move" [] []
      [ Arg "src" $ polyReg "a" Word Load
      , Arg "dst" $ polyReg "a" Word Store ]

    -- Write a constant into a register.
  , Insn "MoveI32" [] []
      [ Arg "i32" $ Imm I32
      , Arg "dst" $ reg Word Store ]

    -- Add a register to another register
  , Insn "Add" [] ["Addable a b"]
      [ Arg "src1" $ polyReg "b" Word Load
      , Arg "src2" $ polyReg "a" Word Load
      , Arg "dst" $ polyReg "a" Word Store ]

    -- Add a constant to a register.
  , Insn "AddI32" [] ["Addable a 'Word"]
      [ Arg "i32" $ Imm I32
      , Arg "src" $ polyReg "a" Word Load
      , Arg "dst" $ polyReg "a" Word Store ]

    -- Subtract a register from a register.
  , Insn "Sub" [] ["Subable a b"]
      [ Arg "src1" $ polyReg "b" Word Load
      , Arg "src2" $ polyReg "a" Word Load
      , Arg "dst" $ polyReg "(Difference a b)" Word Store ]

    -- Decode a Nat from memory into a register.
  , Insn "InputNat" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load
      , Arg "dst" $ reg Word Store ]

    -- Advance begin by size bytes, and bounds-check against end
  , Insn "InputBytes" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load
      , Arg "size" $ reg Word Load ]

    -- Validate and skip over an encoded UTF8 string
  , Insn "InputSkipUntrustedString" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load ]

    -- Check that the input starts with the given literal, and then skip past it
  , Insn "InputShiftLit" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load
      , Arg "lit" $ Imm ImmLit
      , Arg "match" $ reg Word Store ]

    -- Check that the input starts with the given byte sequence, and
    -- then skip past it
  , Insn "InputShiftOutput" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load
      , Arg "output" $ reg BinaryOutputPtr Load
      , Arg "match" $ reg Word Store ]

    -- Decode a Nat from memory
  , Insn "InputSkipNat" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load ]

    -- Skip over an encoded UTF8 string in a binary::Input. The string must be
    -- valid (this is not checked).
  , Insn "InputSkipTrustedString" [] []
      [ Arg "begin" $ reg DataPtr Update
      , Arg "end" $ reg DataPtr Load ]

    -- Reset a binary::Output
  , Insn "ResetOutput" [] []
      [ Arg "output" $ reg BinaryOutputPtr Load ]

    -- Encode a Nat in a register and store it in a binary::Output.
  , Insn "OutputNat" [] []
      [ Arg "src" $ reg Word Load
      , Arg "output" $ reg BinaryOutputPtr Load ]

    -- Encode an immediate Nat and store it in a binary::Output.
  , Insn "OutputNatImm" [] []
      [ Arg "src" $ Imm U32
      , Arg "output" $ reg BinaryOutputPtr Load ]

    -- Encode a byte in a binary::Output
  , Insn "OutputByteImm" [] []
      [ Arg "src" $ Imm U8
      , Arg "output" $ reg BinaryOutputPtr Load ]

    -- Write a sequence of bytes to a binary::Output.
  , Insn "OutputBytes" [] []
      [ Arg "ptr" $ reg DataPtr Load
      , Arg "end" $ reg DataPtr Load
      , Arg "output" $ reg BinaryOutputPtr Load ]

    -- String toLower
  , Insn "OutputStringToLower" [] []
      [ Arg "begin" $ reg DataPtr Load
      , Arg "end" $ reg DataPtr Load
      , Arg "dst" $ reg BinaryOutputPtr Load ]

    -- converts [RelByteSpan] to [ByteSpan]
  , Insn "OutputRelToAbsByteSpans" [] []
      [ Arg "begin" $ reg DataPtr Load
      , Arg "end" $ reg DataPtr Load
      , Arg "dst" $ reg BinaryOutputPtr Load ]

    -- Get the contents of a binary::Output as a pointer and
    -- length. Note that these are only valid until the next operation
    -- on the binary::Output
  , Insn "GetOutput" [] []
      [ Arg "output" $ reg BinaryOutputPtr Load
      , Arg "ptr" $ reg DataPtr Store
      , Arg "end" $ reg DataPtr Store ]

    -- Get the number of bytes in the output
  , Insn "GetOutputSize" [] []
      [ Arg "output" $ reg BinaryOutputPtr Load
      , Arg "dst" $ reg Word Store ]

    -- Load the address and size of a literal
  , Insn "LoadLiteral" [] []
      [ Arg "lit" $ Imm ImmLit
      , Arg "ptr" $ reg DataPtr Store
      , Arg "end" $ reg DataPtr Store ]

  , Insn "LoadLabel" [] []
      [ Arg "lbl" $ Imm ImmOffset
      , Arg "dst" $ reg Offset Store ]

    -- Unconditional jump.
  , Insn "Jump" [EndBlock] []
      [ Arg "tgt" $ Imm ImmOffset ]

  , Insn "JumpReg" [EndBlock] []
      [ Arg "tgt" $ reg Offset Load ]

    -- Jump if a register is 0.
  , Insn "JumpIf0" [] []
      [ Arg "reg" $ reg Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if a register is not 0.
  , Insn "JumpIfNot0" [] []
      [ Arg "reg" $ reg Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if two registers are equal.
  , Insn "JumpIfEq" [] []
      [ Arg "reg1" $ polyReg "a" Word Load
      , Arg "reg2" $ polyReg "a" Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if two registers are not equal.
  , Insn "JumpIfNe" [] []
      [ Arg "reg1" $ polyReg "a" Word Load
      , Arg "reg2" $ polyReg "a" Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if a > b.
  , Insn "JumpIfGt" [] ["Ordered a"]
      [ Arg "reg1" $ polyReg "a" Word Load
      , Arg "reg2" $ polyReg "a" Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if a >= b.
  , Insn "JumpIfGe" [] ["Ordered a"]
      [ Arg "reg1" $ polyReg "a" Word Load
      , Arg "reg2" $ polyReg "a" Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if a < b.
  , Insn "JumpIfLt" [] ["Ordered a"]
      [ Arg "reg1" $ polyReg "a" Word Load
      , Arg "reg2" $ polyReg "a" Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Jump if a <= b.
  , Insn "JumpIfLe" [] ["Ordered a"]
      [ Arg "reg1" $ polyReg "a" Word Load
      , Arg "reg2" $ polyReg "a" Word Load
      , Arg "tgt" $ Imm ImmOffset ]

    -- Decrement the value in a register and jump if it isn't 0.
  , Insn "DecrAndJumpIfNot0" [] []
      [ Arg "reg" $ reg Word Update
      , Arg "tgt" $ Imm ImmOffset ]

    -- Decrement the value in a register and jump if it is 0.
  , Insn "DecrAndJumpIf0" [] []
      [ Arg "reg" $ reg Word Update
      , Arg "tgt" $ Imm ImmOffset ]

  , Insn "SysCall" [] []
      [ Arg "num" $ Imm U8
      , Arg "args" Regs
      ]

    -- Indexed jump - the register contains an index into the array of
    -- offsets. Does nothing if the index is out of range.
  , Insn "Select" [] []
      [ Arg "sel" $ reg Word Load
      , Arg "tgts" Offsets ]

    -- Raise an exception.
  , Insn "Raise" [EndBlock] []
      [ Arg "msg" $ Imm ImmLit ]

    -- For debugging
  , Insn "Trace" [] []
      [ Arg "msg" $ Imm ImmLit ]

  , Insn "TraceReg" [] []
      [ Arg "msg" $ Imm ImmLit
      , Arg "reg" $ reg Word Load ]

    -- Adjust PC to point to 'cont' and suspend execution. The first argument
    -- is a temporary, unused left-over for backwards compatibility.
  , Insn "Suspend" [EndBlock, Return] []
      [ Arg "cont" $ Imm ImmOffset ]

    -- Return from a subroutine.
  , Insn "Ret" [EndBlock, Return] [] []

  ]

--- Packed representation

-- | Operands: args with their start bit index (little endian) and sizes in bits
data Opd = Opd
  { opdArg :: Arg
  , opdStart :: Int
  , opdSize :: Int
  }

-- | Determine the 'Insn's operands
insnOperands :: Insn -> [Opd]
insnOperands Insn{..} = zipWith3 Opd insnArgs starts sizes
  where
    sizes = map (argSize . argTy) insnArgs
    starts = scanl' (+) 8 sizes

-- | Get the size of an argument in the Insn word in bits
argSize :: ArgTy -> Int
argSize (Imm ty) = immSize ty
argSize Reg{} = 12
argSize Offsets = 32
argSize Regs = 32

-- | Get the size of an immediate operand in the Insn word in bits
immSize :: ImmTy -> Int
immSize U8 = 8
immSize I8 = 8
immSize U32 = 32
immSize I32 = 32
immSize ImmOffset = 32
immSize ImmLit = 12

-- | Get the number of unused bits (always the most significant ones) in an
-- 'Insn'
insnUsedBits :: Insn -> Int
insnUsedBits insn  = sum (map (argSize . argTy) $ insnArgs insn) + 8
