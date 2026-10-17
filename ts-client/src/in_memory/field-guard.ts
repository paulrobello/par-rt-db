import { RtDbError } from "../errors.js";
import type {
  FieldExpectationJson,
  FieldTypeJson,
  TableJson,
  TransactionJson,
} from "../protocol.js";

const encoder = new TextEncoder();
const isObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

export function validateFieldGuardShapes(txn: TransactionJson): void {
  for (const step of txn.steps) {
    if (step.op === "expectFields") {
      if (
        Object.keys(step).sort().join(",") !== "expected,id,op,table" ||
        typeof step.table !== "string" ||
        typeof step.id !== "string" ||
        !Array.isArray(step.expected) ||
        step.expected.some(
          (item) =>
            !isObject(item) ||
            Object.keys(item).sort().join(",") !== "path,value" ||
            !Array.isArray(item.path) ||
            item.path.some((key: unknown) => typeof key !== "string"),
        )
      )
        throw new RtDbError("BAD_REQUEST", "malformed expectFields step");
    } else if (step.op === "schedule") {
      validateFieldGuardShapes(step.txn);
    } else if (step.op === "startWorkflow") {
      for (const child of step.spec.steps) if (child.txn) validateFieldGuardShapes(child.txn);
    }
  }
}

function declaredPath(fields: Record<string, FieldTypeJson>, path: string[]): boolean {
  return Object.hasOwn(fields, path[0]) && declaredChild(fields[path[0]], path.slice(1));
}

function declaredChild(field: FieldTypeJson, path: string[]): boolean {
  if (path.length === 0) return true;
  switch (field.type) {
    case "any":
      return true;
    case "optional":
      return declaredChild(field.inner, path);
    case "object":
      return declaredPath(field.fields, path);
    case "record":
      return declaredChild(field.value, path.slice(1));
    case "union":
      return field.variants.some((variant) => declaredChild(variant, path));
    default:
      return false;
  }
}

function jsonEqual(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (Array.isArray(left) && Array.isArray(right))
    return (
      left.length === right.length && left.every((value, index) => jsonEqual(value, right[index]))
    );
  if (isObject(left) && isObject(right)) {
    const keys = Object.keys(left);
    return (
      keys.length === Object.keys(right).length &&
      keys.every((key) => Object.hasOwn(right, key) && jsonEqual(left[key], right[key]))
    );
  }
  return false;
}

/** Call after the live row is located, matching the server's visibility-before-validation contract. */
export function checkFieldExpectations(
  table: TableJson,
  doc: Record<string, unknown>,
  expected: FieldExpectationJson[],
): void {
  if (expected.length === 0 || expected.length > 64)
    throw new RtDbError("BAD_REQUEST", "expected must contain 1-64 field assertions");
  const paths = new Set<string>();
  let nodes = 0;
  let bytes = 0;
  const invalidBudget = (): never => {
    throw new RtDbError(
      "BAD_REQUEST",
      "expected field values exceed the depth, node or UTF-8 byte budget",
    );
  };
  const budget = (value: unknown, depth: number): void => {
    nodes += 1;
    if (depth > 32 || nodes > 8192 || bytes > 65_536) invalidBudget();
    if (typeof value === "string") bytes += encoder.encode(value).length;
    else if (Array.isArray(value)) for (const child of value) budget(child, depth + 1);
    else if (isObject(value)) {
      const prototype: unknown = Object.getPrototypeOf(value);
      if (prototype !== Object.prototype && prototype !== null) invalidBudget();
      for (const [key, child] of Object.entries(value)) {
        bytes += encoder.encode(key).length;
        budget(child, depth + 1);
      }
    } else if (
      value !== null &&
      typeof value !== "boolean" &&
      !(typeof value === "number" && Number.isFinite(value))
    )
      invalidBudget();
    if (bytes > 65_536) invalidBudget();
  };
  for (const item of expected) {
    if (
      item.path.length === 0 ||
      item.path.length > 16 ||
      item.path.some((key) => key.length === 0 || encoder.encode(key).length > 256)
    )
      throw new RtDbError("BAD_REQUEST", "field paths require 1-16 keys of 1-256 UTF-8 bytes");
    const path = JSON.stringify(item.path);
    if (paths.has(path))
      throw new RtDbError("BAD_REQUEST", "expected contains a duplicate field path");
    paths.add(path);
    bytes += item.path.reduce((sum, key) => sum + encoder.encode(key).length, 0);
    budget(item.value, 0);
    if (!declaredPath(table.fields, item.path))
      throw new RtDbError("SCHEMA_VIOLATION", "expected field path is not declared");
  }
  for (const item of expected) {
    let value: unknown = doc;
    for (const key of item.path) {
      if (!isObject(value) || !Object.hasOwn(value, key))
        throw new RtDbError("PRECONDITION_FAILED", "expected fields did not match");
      value = value[key];
    }
    if (!jsonEqual(value, item.value))
      throw new RtDbError("PRECONDITION_FAILED", "expected fields did not match");
  }
}
