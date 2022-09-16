{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE RecursiveDo #-}
module Glean.RTS.Typecheck
  ( CompiledTypecheck
  , checkType
  , checkSignature
  , randomFact
  ) where

import Control.Monad

import Glean.Angle.Types hiding (Type)
import Glean.Bytecode.Types
import Glean.RTS.Foreign.Bytecode
import Glean.RTS.Types
import Glean.RTS.Bytecode.Code
import Glean.RTS.Bytecode.Gen.Issue

-- | Type tag for Subroutine
data CompiledTypecheck

typecheck
  :: Register ('Fun '[ 'Word, 'Word, 'WordPtr ])
  -> Register 'DataPtr
  -> Register 'DataPtr
  -> Register 'BinaryOutputPtr
  -> Type
  -> Code ()
typecheck rename input inputend output = tc
  where
    tc ByteTy = do
      size <- constant 1
      local $ \ptr -> do
        move input ptr
        inputBytes input inputend size
        outputBytes ptr input output
    tc NatTy = local $ \reg -> do
      inputNat input inputend reg
      outputNat reg output
    tc StringTy =
      local $ \ptr -> do
        move input ptr
        inputSkipUntrustedString input inputend
        outputBytes ptr input output
    tc (ArrayTy elty) = local $ \size -> do
      inputNat input inputend size
      outputNat size output
      case derefType elty of
        ByteTy -> local $ \ptr -> do
          move input ptr
          inputBytes input inputend size
          outputBytes ptr input output
        _ -> mdo
          jumpIf0 size end
          loop <- label
          tc elty
          decrAndJumpIfNot0 size loop
          end <- label
          return ()
    tc (RecordTy fields) = mapM_ (tc . fieldDefType) fields
    tc (SumTy fields) = mdo
      local $ \sel -> do
        inputNat input inputend sel
        outputNat sel output
        select sel alts
      raise "selector out of range"
      alts <- forM fields $ \(FieldDef _ ty) -> do
        alt <- label
        tc ty
        jump end
        return alt
      end <- label
      return ()
    tc (PredicateTy (PidRef (Pid pid) _)) = local $ \ide -> do
      t <- constant $ fromIntegral pid
      inputNat input inputend ide
      callFun_2_1 rename ide t ide
      outputNat ide output
    tc (NamedTy (ExpandedType _ ty)) = tc ty
    tc (MaybeTy ty) = mdo
      local $ \sel -> do
        inputNat input inputend sel
        outputNat sel output
        select sel [end,just]
      raise "maybe selector out of range"
      just <- label
      tc ty
      end <- label
      return ()
    tc (EnumeratedTy names) = tcEnum $ fromIntegral $ length names
    tc BooleanTy = tcEnum 2

    tcEnum arity = mdo
      k <- constant arity
      local $ \sel -> do
        inputNat input inputend sel
        outputNat sel output
        jumpIfLt sel k end
        raise "selector out of range"
      end <- label
      return ()

-- | Generate a subroutine which typechecks and substitutes a value. It has
-- the following arguments:
--
-- std::function<Id(Id id, Id type)> - fact substitution
-- binary::Input * - value
-- binary::Output * - substituted value
--
checkType :: Type -> IO (Subroutine CompiledTypecheck)
checkType ty = checkSignature ty $ RecordTy []

-- | Generate a subroutine which typechecks and substitutes a clause. It has
-- the following arguments:
--
-- std::function<Id(Id id, Id type)> - fact substitution
-- const void * - begin of clause/key
-- const void * - end of key/begin of value
-- const void * - end of clause/value
--
-- It returns the substituted clause in the first output and size of the
-- substituted key in the first local register.
--
checkSignature :: Type -> Type -> IO (Subroutine CompiledTypecheck)
checkSignature key_ty val_ty =
  generate Optimised $
    \rename clause_begin key_end clause_end -> output $ \out ->
    -- We return the key size in the first local register
    local $ \key_size -> mdo
    typecheck rename clause_begin key_end out key_ty
    check "key" clause_begin key_end
    getOutputSize out key_size
    typecheck rename clause_begin clause_end out val_ty
    check "value" clause_begin clause_end
    ret
  where
  check str reg1 reg2 = mdo
    jumpIfNe (castRegister reg1) (castRegister reg2) bad
    -- The basic block layout algorithm will make the happy path sequential and
    -- delete this jump.
    jump ok
    bad <- label
    raise $ "extra bytes at end of " <> str
    ok <- label
    return ()

randomValue
  :: Register ('Fun '[ 'Word, 'WordPtr ])
  -> Register ('Fun '[ 'WordPtr ])
  -> Register ('Fun '[ 'Word, 'WordPtr ])
  -> Register ('Fun '[ 'Word ])
  -> Register 'BinaryOutputPtr
  -> Type
  -> Code ()
randomValue gen_fact gen_length gen_nat gen_string output = gen
  where
    gen ByteTy = local $ \reg -> do
      c <- constant 128
      callFun_1_1 gen_nat c reg
      outputNat reg output
    gen NatTy = local $ \reg -> do
      c <- constant 0
      callFun_1_1 gen_nat c reg
      outputNat reg output
    gen StringTy = callFun_1_0 gen_string $ castRegister output
    gen (ArrayTy elty) = local $ \size -> mdo
      callFun_0_1 gen_length size
      outputNat size output
      jumpIf0 size end
      loop <- label
      gen elty
      decrAndJumpIfNot0 size loop
      end <- label
      return ()
    gen (RecordTy fields) = mapM_ (gen . fieldDefType) fields
    gen (SumTy fields) = mdo
      local $ \sel -> do
        c <- constant $ fromIntegral $ length fields
        callFun_1_1 gen_nat c sel
        outputNat sel output
        select sel alts
      alts <- forM fields $ \(FieldDef _ ty) -> do
        alt <- label
        gen ty
        jump end
        return alt
      end <- label
      return ()
    gen (PredicateTy (PidRef (Pid pid) _)) = local $ \ide -> do
      t <- constant $ fromIntegral pid
      callFun_1_1 gen_fact t ide
      outputNat ide output
    gen (NamedTy (ExpandedType _ ty)) = gen ty
    gen (MaybeTy ty) = mdo
      c <- constant 2
      local $ \sel -> do
        callFun_1_1 gen_nat c sel
        outputNat sel output
        jumpIf0 sel end
      gen ty
      end <- label
      return ()
    gen (EnumeratedTy names) = tcEnum $ fromIntegral $ length names
    gen BooleanTy = tcEnum 2

    tcEnum arity = local $ \sel -> do
      k <- constant arity
      callFun_1_1 gen_nat k sel
      outputNat sel output

randomFact :: Type -> Type -> IO (Subroutine ())
randomFact key_ty val_ty =
  generate Optimised $
    \gen_fact gen_size gen_nat gen_string -> output $ \out ->
    local $ \key_size -> mdo
    randomValue gen_fact gen_size gen_nat gen_string out key_ty
    getOutputSize out key_size
    randomValue gen_fact gen_size gen_nat gen_string out val_ty
    ret
    