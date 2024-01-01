
// recursive JSON type; you can append nullable or not
export type JsonType = string | number | boolean | { [x: string]: JsonType } | Array<JsonType>;
    