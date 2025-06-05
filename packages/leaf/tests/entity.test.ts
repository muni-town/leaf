import { describe, it, expect } from "vitest";
import { Entity, defComponent, LoroMap, Marker } from "../src";

describe("Entity", () => {
  const Name = defComponent(
    "name:01JNVY76XPH6Q5AVA385HP04G7",
    LoroMap<{ first: string; last?: string }>,
    (map) => map.set("first", "unnamed")
  );

  const SoftDelete = defComponent(
    "softDelete:01JPFQ2MXH4XJVTDT5ATEBQBQA",
    Marker
  );

  it("should create a new entity", () => {
    const entity = new Entity();
    expect(entity).toBeDefined();
    expect(entity.id).toBeDefined();
  });

  it("should initialize and check components", () => {
    const entity = new Entity();
    entity.init(Name);
    expect(entity.has(Name)).toBe(true);
    expect(entity.has(SoftDelete)).toBe(false);
  });

  it("should initialize component with default values", () => {
    const entity = new Entity();
    entity.init(Name);

    entity.getOrInit(Name, (name) => {
      expect(name.get("first")).toBe("unnamed");
      return name;
    });
  });

  it("should handle component deletion", () => {
    const entity = new Entity();
    entity.init(Name);
    expect(entity.has(Name)).toBe(true);

    entity.delete(Name);
    expect(entity.has(Name)).toBe(false);
  });

  it("should handle marker components", () => {
    const entity = new Entity();
    entity.init(SoftDelete);
    expect(entity.has(SoftDelete)).toBe(true);

    entity.delete(SoftDelete);
    expect(entity.has(SoftDelete)).toBe(false);
  });

  it("should handle component updates", () => {
    const entity = new Entity();
    entity.init(Name);

    entity.getOrInit(Name, (name) => {
      name.set("first", "John");
      name.set("last", "Doe");
      return name;
    });

    entity.getOrInit(Name, (name) => {
      expect(name.get("first")).toBe("John");
      expect(name.get("last")).toBe("Doe");
      return name;
    });
  });
});
