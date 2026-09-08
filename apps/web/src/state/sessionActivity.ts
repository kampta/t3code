import { createActiveSessionCountAtom } from "@t3tools/client-runtime/state/session-activity";

import { environmentCatalog } from "../connection/catalog";
import { environmentShell } from "./shell";

export const activeSessionCountAtom = createActiveSessionCountAtom({
  catalogValueAtom: environmentCatalog.catalogValueAtom,
  shellStateValueAtom: environmentShell.stateValueAtom,
});
