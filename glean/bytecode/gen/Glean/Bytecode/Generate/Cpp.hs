{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.Bytecode.Generate.Cpp (main)
where

import Data.Bits
import Data.List (intercalate, stripPrefix)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import Data.Word (Word64)
import Numeric (showHex)
import System.Directory
import System.Environment
import System.Exit (die)
import System.FilePath

import Glean.Bytecode.Generate.Instruction
import Glean.Bytecode.Types

-- We generate 3 files:
--
-- instruction.h has the enum with all opcodes
--
-- evaluator.h defines functions for decoding instructions and two evaluators -
-- one based on switch and the other token-threaded. It is intended to be
-- included as part of the definition of an evaluator class.

indent :: Text -> Text
indent x = "  " <> x

main :: IO ()
main = do
  args <- getArgs
  dir <- case args of
    [arg] | Just dir <- stripPrefix "--install_dir=" arg ->
      return $ dir </> "bytecode/gen"
    _ -> die "invalid arguments"
  createDirectoryIfMissing True dir
  genHeader (dir </> "instruction.h")
    ["#include <cstdint>"]
    genOpEnum
  genFile (dir </> "evaluate.h")
    genEvaluator

-- | Generate a file.
genFile :: FilePath -> [Text] -> IO ()
genFile path ls = Text.writeFile path
  $ Text.unlines
  $ ("// @" <> "generated") : "" : ls

-- | Generate a header file (with #pragma once and namespaces).
genHeader :: FilePath -> [Text] -> [Text] -> IO ()
genHeader path pre ls = genFile path $
  [ "#pragma once"
  , "" ]
  ++ pre ++
  [ ""
  , "namespace facebook {"
  , "namespace glean {"
  , "namespace rts {"
  , "" ]
  ++ ls ++
  [ ""
  , "}"
  , "}"
  , "}" ]

unusedOps :: [Text]
unusedOps = ["Unused" <> Text.pack (show n) | n <- [length instructions .. 255]]

-- | Generate the enum with all opcodes.
genOpEnum :: [Text]
genOpEnum =
  "enum class Op : uint8_t {"
  : [indent $ insnName insn <> "," | insn <- instructions]
  ++ [indent $ op <> "," | op <- unusedOps]
  ++ ["};"]

genEvaluator :: [Text]
genEvaluator =
  intercalate [""] (map (map indent) $
    genEvalSwitch : genEvalIndirect : map genInsnEval instructions)

-- | Generate a method which decodes and then executes (via a function which
-- we expect to be defined) an instruction. For each instruction, we generate
-- a struct containing its decoded arguments and adjust PC to point to the next
-- instruction in the stream.
genInsnEval :: Insn -> [Text]
genInsnEval insn@Insn{..} =
  [ "struct " <> insnName <> " {" ]
  ++ map indent (concatMap declare insnArgs) ++
  [ "};"
  , ""
  , "FOLLY_ALWAYS_INLINE " <> retType <> " eval_" <> insnName <> "(uint64_t insn) {"
  , "  " <> insnName <> " args;" ]

  -- Telling the compiler that the upper bits are 0 lets it omit the mask for
  -- the last operand
  ++ ["  folly::assume((insn >> " <> Text.pack (show used) <> ") == 0);"
      | let used = insnUsedBits insn, used /= 64 ]

  ++ [indent line | opd <- insnOperands insn, line <- decode opd ] ++
  [ "  return execute(args);"
  , "}" ]
  where
    retType
      | Return `elem` insnEffects = "const uint64_t * FOLLY_NULLABLE "
      | otherwise = "void"

    declare (Arg name (Imm ty)) =
      [ immType ty <> " " <> name <> ";" ]
    declare (Arg name (Reg _ ty Load)) =
      [ cppType ty <> " " <> name <> ";" ]
    -- just make a pointer to the register for Store and Update for now
    declare (Arg name (Reg _ ty _)) =
      [ "Reg<" <> cppType ty <> "> " <> name <> ";" ]
    declare (Arg name Offsets) =
      [ "uint32_t " <> name <> "_size;"
      , "const uint64_t *" <> name <> ";" ]
    declare (Arg name Regs) =
      [ "uint32_t " <> name <> "_size;"
      , "const uint64_t *" <> name <> ";" ]

    decode Opd{..} = assign opdArg $
      "(insn >> " <> Text.pack (show opdStart) <> ") & 0x"
        <> Text.pack (showHex (1 `shiftL` opdSize - 1 :: Word64) "")
        <> "U"

    assign (Arg name ty) val =
      ( "args." <> name <> " = " <> load ty val <> ";" ) : rest name ty val

    load (Imm ImmLit) val = "&literals[" <> val <> "]"
    load (Imm ty) val = "static_cast<" <> immType ty <> ">(" <> val <> ")"
    load (Reg _ ty Load) val =
      "Reg<" <> cppType ty <> ">(&frame[" <> val <> "]).get()"
    load (Reg _ ty _) val = "Reg<" <> cppType ty <> ">(&frame[" <> val <> "])"
    load Offsets _ = "pc"
    load Regs _ = "pc"

    rest name Offsets val =
      [ "args." <> name <> "_size = " <> val <> ";"
      , "pc += args." <> name <> "_size;" ]
    rest name Regs val =
      [ "args." <> name <> "_size = " <> val <> ";"
      , "pc += args." <> name <> "_size;" ]
    rest _ _ _ = []

cppType :: Ty -> Text
cppType DataPtr = "const unsigned char *"
cppType Lit = "const std::string *"
cppType WordPtr = "uint64_t *"
cppType BinaryOutputPtr = "binary::Output *"
cppType _ = "uint64_t"

immType :: ImmTy -> Text
immType U8 = "uint8_t"
immType I8 = "int8_t"
immType U32 = "uint32_t"
immType I32 = "int32_t"
immType ImmOffset = "int32_t"
immType ImmLit = "const std::string *"

-- | Generate a switch-based evaluator.
--
-- while(true) {
--   switch (static_cast<Op>(*pc++)) {
--     case Op::Name:
--       eval_Name();
--       break;
--      ...
--
--     case Op::Unused42:
--     ...
--     case Op::Unused255:
--       rts::error("invalid opcode");
--    }
-- }
--
-- Note that we generate alternatives for each possible value of the opcode byte
-- rather than using `default` because this results in better code (jump via
-- table at end of each alternative rather than bounds check + jump via table in
-- a common loop).
--
genEvalSwitch :: [Text]
genEvalSwitch =
  [ "FOLLY_ALWAYS_INLINE const uint64_t * FOLLY_NULLABLE evalSwitch() {"
  , "  while (true) {"
  , "    const uint64_t insn = *pc++;"
  , "    switch (static_cast<Op>(insn & 0xff)) {" ]
  ++ intercalate [""] (map genAlt instructions)
  ++
  [ "" ]
  ++ map genUnusedAlt unusedOps ++
  [ "        rts::error(\"invalid opcode\");"
  , "    }"
  , "  }"
  , "}" ]
  where
    genAlt insn =
      "      case Op::" <> insnName insn <> ":"
      : makeCall insn "        break;"

    genUnusedAlt op =
      "      case Op::" <> op <> ":"

-- | Generate a token-threaded interpreter (opcode are indices into a table
-- of labels, dispatch via compute goto). Note dispatch is repeated for each
-- instruction for (perhaps) better branch prediction.
--
-- static const void * const labels[] = { &&label_Name, ... };
--
-- goto *labels[*pc++];
--
-- label_Name:
--   eval_Name();
--   goto *labels[*pc++];
--
genEvalIndirect :: [Text]
genEvalIndirect =
  [ "FOLLY_ALWAYS_INLINE const uint64_t * FOLLY_NULLABLE evalIndirect() {"
  , "  static const void * const labels[] = {" ]
  ++
  [ "    &&label_" <> insnName insn <> "," | insn <- instructions ]
  ++
  [ "  };"
  , ""
  , "  uint64_t insn;"
  , ""
  , dispatch
  , "" ]
  ++ intercalate [""] (map genAlt instructions) ++
  ["}"]
  where
    dispatch = "  insn = *pc++; goto *labels[insn&0xff];"

    genAlt insn =
      "label_" <> insnName insn <> ":"
      : makeCall insn dispatch

makeCall :: Insn -> Text -> [Text]
makeCall insn cont
  | Return `elem` insnEffects insn =
      [ "        return " <> call ]
  | otherwise =
      [ "        " <> call
      , cont ]
  where
    call = "eval_" <> insnName insn <> "(insn);"
