# @generated
# To regenerate this file run fbcode//glean/schema/gen/sync
from typing import Optional, Tuple, Union
import json
from thrift.py3 import Struct
from glean.schema.py.glean_schema_predicate import GleanSchemaPredicate


from glean.schema.codemarkup.types import (
    FileEntityKinds,
    FileEntityXRefKinds,
    EntityAnnotations,
    SearchRelatedEntities,
    EntityVisibility,
    FileEntityInfos,
    FileEntityXRefInfos,
    EntityLocation,
    ExtendsChildEntity,
    EntityToAnnotations,
    EntityInfo,
    ExtendsParentEntity,
    ContainsChildEntity,
    FileEntityXRefLocations,
    FileEntityXRefSpans,
    EntityUses,
    FileEntityLocations,
    EntityReferences,
    EntityKind,
    ResolveLocation,
)


class CodemarkupFileEntityKinds(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityKinds.30 {{ }}", FileEntityKinds
    return f"codemarkup.FileEntityKinds.30 {{ file = _, entity = _, kind = _ }}", FileEntityKinds

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None, kind: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityKinds":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupFileEntityXRefKinds(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityXRefKinds.30 {{ }}", FileEntityXRefKinds
    return f"codemarkup.FileEntityXRefKinds.30 {{ file = _, entity = _, kind = _ }}", FileEntityXRefKinds

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None, kind: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityXRefKinds":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityAnnotations(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityAnnotations.30 {{ }}", EntityAnnotations
    return f"codemarkup.EntityAnnotations.30 {json.dumps(key)}", EntityAnnotations

  @staticmethod
  def angle_query(*, arg: Optional[Tuple[()]] = None) -> "CodemarkupEntityAnnotations":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupSearchRelatedEntities(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.SearchRelatedEntities.30 {{ }}", SearchRelatedEntities
    return f"codemarkup.SearchRelatedEntities.30 {{ query = _, parent = _, child = _ }}", SearchRelatedEntities

  @staticmethod
  def angle_query(*, query: Optional[Tuple[()]] = None, parent: Optional[Tuple[()]] = None, child: Optional[Tuple[()]] = None) -> "CodemarkupSearchRelatedEntities":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityVisibility(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityVisibility.30 {{ }}", EntityVisibility
    return f"codemarkup.EntityVisibility.30 {json.dumps(key)}", EntityVisibility

  @staticmethod
  def angle_query(*, arg: Optional[Tuple[()]] = None) -> "CodemarkupEntityVisibility":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupFileEntityInfos(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityInfos.30 {{ }}", FileEntityInfos
    return f"codemarkup.FileEntityInfos.30 {{ file = _, entity = _, info = _ }}", FileEntityInfos

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None, info: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityInfos":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupFileEntityXRefInfos(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityXRefInfos.30 {{ }}", FileEntityXRefInfos
    return f"codemarkup.FileEntityXRefInfos.30 {{ file = _, entity = _, info = _ }}", FileEntityXRefInfos

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None, info: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityXRefInfos":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityLocation(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityLocation.30 {{ }}", EntityLocation
    return f"codemarkup.EntityLocation.30 {{ entity = _, location = _ }}", EntityLocation

  @staticmethod
  def angle_query(*, entity: Optional[Tuple[()]] = None, location: Optional[Tuple[()]] = None) -> "CodemarkupEntityLocation":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupExtendsChildEntity(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.ExtendsChildEntity.30 {{ }}", ExtendsChildEntity
    return f"codemarkup.ExtendsChildEntity.30 {{ parent = _, child = _ }}", ExtendsChildEntity

  @staticmethod
  def angle_query(*, parent: Optional[Tuple[()]] = None, child: Optional[Tuple[()]] = None) -> "CodemarkupExtendsChildEntity":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityToAnnotations(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityToAnnotations.30 {{ }}", EntityToAnnotations
    return f"codemarkup.EntityToAnnotations.30 {{ entity = _, annotations = _ }}", EntityToAnnotations

  @staticmethod
  def angle_query(*, entity: Optional[Tuple[()]] = None, annotations: Optional[Tuple[()]] = None) -> "CodemarkupEntityToAnnotations":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityToAnnotations(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> str:
    return f"codemarkup.EntityToAnnotations.30 { { } }"

  @staticmethod
  def angle_query(*, name: Tuple[()]) -> "CodemarkupEntityToAnnotations":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityInfo(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityInfo.30 {{ }}", EntityInfo
    return f"codemarkup.EntityInfo.30 {{ entity = _, info = _ }}", EntityInfo

  @staticmethod
  def angle_query(*, entity: Optional[Tuple[()]] = None, info: Optional[Tuple[()]] = None) -> "CodemarkupEntityInfo":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupExtendsParentEntity(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.ExtendsParentEntity.30 {{ }}", ExtendsParentEntity
    return f"codemarkup.ExtendsParentEntity.30 {{ child = _, parent = _ }}", ExtendsParentEntity

  @staticmethod
  def angle_query(*, child: Optional[Tuple[()]] = None, parent: Optional[Tuple[()]] = None) -> "CodemarkupExtendsParentEntity":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupContainsChildEntity(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.ContainsChildEntity.30 {{ }}", ContainsChildEntity
    return f"codemarkup.ContainsChildEntity.30 {{ parent = _, child = _ }}", ContainsChildEntity

  @staticmethod
  def angle_query(*, parent: Optional[Tuple[()]] = None, child: Optional[Tuple[()]] = None) -> "CodemarkupContainsChildEntity":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupFileEntityXRefLocations(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityXRefLocations.30 {{ }}", FileEntityXRefLocations
    return f"codemarkup.FileEntityXRefLocations.30 {{ file = _, xref = _, entity = _ }}", FileEntityXRefLocations

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, xref: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityXRefLocations":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupFileEntityXRefSpans(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityXRefSpans.30 {{ }}", FileEntityXRefSpans
    return f"codemarkup.FileEntityXRefSpans.30 {{ file = _, span = _, entity = _ }}", FileEntityXRefSpans

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, span: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityXRefSpans":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityUses(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityUses.30 {{ }}", EntityUses
    return f"codemarkup.EntityUses.30 {{ target = _, file = _, span = _ }}", EntityUses

  @staticmethod
  def angle_query(*, target: Optional[Tuple[()]] = None, file: Optional[Tuple[()]] = None, span: Optional[Tuple[()]] = None) -> "CodemarkupEntityUses":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupFileEntityLocations(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.FileEntityLocations.30 {{ }}", FileEntityLocations
    return f"codemarkup.FileEntityLocations.30 {{ file = _, location = _, entity = _ }}", FileEntityLocations

  @staticmethod
  def angle_query(*, file: Optional[Tuple[()]] = None, location: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None) -> "CodemarkupFileEntityLocations":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityReferences(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityReferences.30 {{ }}", EntityReferences
    return f"codemarkup.EntityReferences.30 {{ target = _, file = _, range = _ }}", EntityReferences

  @staticmethod
  def angle_query(*, target: Optional[Tuple[()]] = None, file: Optional[Tuple[()]] = None, range: Optional[Tuple[()]] = None) -> "CodemarkupEntityReferences":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupEntityKind(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.EntityKind.30 {{ }}", EntityKind
    return f"codemarkup.EntityKind.30 {{ entity = _, kind = _ }}", EntityKind

  @staticmethod
  def angle_query(*, entity: Optional[Tuple[()]] = None, kind: Optional[Tuple[()]] = None) -> "CodemarkupEntityKind":
    raise Exception("this function can only be called from @angle_query")

class CodemarkupResolveLocation(GleanSchemaPredicate):
  @staticmethod
  def build_angle(key: Union[int, bool, str, Tuple[()]]) -> Tuple[str, Struct]:
    if key is None:
      return f"codemarkup.ResolveLocation.30 {{ }}", ResolveLocation
    return f"codemarkup.ResolveLocation.30 {{ location = _, entity = _ }}", ResolveLocation

  @staticmethod
  def angle_query(*, location: Optional[Tuple[()]] = None, entity: Optional[Tuple[()]] = None) -> "CodemarkupResolveLocation":
    raise Exception("this function can only be called from @angle_query")


