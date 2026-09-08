import { useAtomValue } from "@effect/atom-react";
import {
  REACTION_IMAGE,
  resolveSessionReaction,
} from "@t3tools/client-runtime/state/session-activity";
import { memo } from "react";

import reactionImage from "../../../../../assets/memes/mcmahon-reaction.png";
import { activeSessionCountAtom } from "../../state/sessionActivity";

const THUMBNAIL_WIDTH = 90;
const IMAGE_SCALE = THUMBNAIL_WIDTH / REACTION_IMAGE.cropWidth;

export const SessionReaction = memo(function SessionReaction() {
  const count = useAtomValue(activeSessionCountAtom);
  const reaction = resolveSessionReaction(count);
  if (reaction === null) return null;

  return (
    <div
      role="img"
      aria-label={`${count} active sessions. Vince McMahon reaction: ${reaction.label}.`}
      aria-description="Working sessions across connected environments. Sessions waiting for approval or input are excluded."
      className="flex min-w-0 items-center gap-3 rounded-lg border border-sidebar-border bg-sidebar-accent/40 p-2"
    >
      <div
        aria-hidden="true"
        className="relative shrink-0 overflow-hidden rounded-md bg-black"
        style={{ width: THUMBNAIL_WIDTH, height: REACTION_IMAGE.cropHeight * IMAGE_SCALE }}
      >
        <img
          src={reactionImage}
          alt=""
          draggable={false}
          decoding="async"
          className="pointer-events-none absolute max-w-none select-none"
          style={{
            width: REACTION_IMAGE.width * IMAGE_SCALE,
            height: REACTION_IMAGE.height * IMAGE_SCALE,
            left: -REACTION_IMAGE.cropLeft * IMAGE_SCALE,
            top: -reaction.cropTop * IMAGE_SCALE,
          }}
        />
      </div>
      <div aria-hidden="true" className="min-w-0">
        <p className="text-xs font-medium text-sidebar-foreground">
          <span className="tabular-nums">{count}</span> active sessions
        </p>
        <p className="mt-1 text-[11px] text-sidebar-muted-foreground">{reaction.label}</p>
      </div>
    </div>
  );
});
