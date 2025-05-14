import {
  LoroCounter,
  LoroList,
  LoroMap,
  LoroMovableList,
  LoroText,
  LoroTree,
} from "loro-crdt";

/** A Loro CRDT type that may be used as the type of a {@link defComponent|component}. */
export type ComponentType =
  | LoroCounter
  | LoroList
  | LoroMap
  | LoroMovableList
  | LoroText
  | LoroTree
  | Marker;
/** A constructor for a {@linkcode ComponentType}. */
export type ComponentConstructor<T extends ComponentType> = new () => T;

/**
 * A {@linkcode ComponentType} that doesn't have any data.
 *
 * This is useful for making "marker" components where you only care whether or not the component is
 * added to an entity and there is no other data to store in the component itself.
 * */
export class Marker {
  constructor() {}
}

/** The ID for a component. */
export type ComponentId = string;

/**
 * A component definition.
 *
 * Component definitions have the information needed to get and initialize the component on an Entity.
 *
 * Use {@linkcode defComponent} to create a new {@linkcode ComponentDef}.
 */
export type ComponentDef<T extends ComponentType> = {
  id: ComponentId;
  constructor: ComponentConstructor<T>;
  init: (container: T) => void;
};

/**
 * Define a new component type.
 *
 * All data in Leaf is made up of components. These components are meant to be small, re-usable, and
 * semantically meaningful pieces of data, such as a `Name`, `Image`, or `Description`.
 *
 * Before you can use a component you must define it, which sets its unique ID, its type, and its
 * initialization function.
 *
 * ## Example
 *
 * Here's an example of a `Name` component that requires a first name and optionally has a last
 * name.
 *
 * ```ts
 * export const Name = defComponent(
 *    "name:01JNVY76XPH6Q5AVA385HP04G7",
 *    LoroMap<{ first: string; last?: string }>,
 *    (map) => map.set("first", "")
 * );
 * ```
 *
 * After defining a component you may use it with an {@linkcode Entity}.
 *
 * Let's break it down piece-by-piece.
 *
 * #### Exported Variable
 * Notice that we store the definition in a variable and we export it from the module. This allows
 *   us to use the same component definition throughout the codebase, and even in other projects
 *   that might depend on this module, if we are writing a library.
 *
 * This is not required, of course, but will often be useful in real projects.
 *
 * #### Unique ID
 * The unique ID makes sure that this component is distinguished from any other component that might
 *   exist. By using exported variables and unique IDs, we don't have to worry about conflicting
 *   names for different components.
 *
 * #### Component Type
 * The next argument is the type we want to use for the component. This must be one of the
 *   {@linkcode ComponentType}s and should usually include extra type annotations describing the
 *   data that will go inside.
 *
 * In this example we say that `Name` is a {@linkcode LoroMap} and we annotate the inner data as
 * requiring a `string` `first` name and optionally having a `string` `last` name.
 *
 * Note that components must be a so called "container type", such as a map, array, rich text, etc.
 * If you wish to store only one primitive value such as a `string` or `number` in a component, you
 * can always store it in a map with one field.
 *
 * #### Initialization Function
 *
 * Finally, the last argument is an initialization function. When the component gets created it will
 * be the empty, default value of whatever {@linkcode ComponentType} that you specified.
 *
 * In this case, that means `Name` will start off as an empty map which, notably, does not match our
 * type annotation of requiring a first name.
 *
 * Therefore it is important, when annotating your component type, that you also supply an
 * initialization function to set any required fields so that your returned component will match
 * your annotated type.
 *
 * In this case, we just initialize the first name to an empty string.
 *
 * @param id The globally unique ID of the component type. Often this will include a
 * [ULID](https://ulidgenerator.com/) or [UUID](https://www.uuidgenerator.net/) prefixed by a short
 * human-readable name to assist in debugging.
 * @param constructor One of the {@linkcode ComponentType}s. This will be used to construct the
 * component initially, in addition to the `init` function if provided.
 * @param init An optional function that will be called to initialize the component when it is first
 * added to an Entity. Since the constructor will initialize an empty version of whatever type you
 * select, you must use this init function if you want to make sure that it has any of the initial
 * data necessary to match your annotated type.
 * @returns A component definition that can be used to add, edit, and delete components on an
 * {@linkcode Entity}.
 */
export function defComponent<T extends ComponentType>(
  id: string,
  constructor: ComponentConstructor<T>,
  init: (container: T) => void = () => {}
): ComponentDef<T> {
  return {
    id,
    constructor,
    init,
  };
}
