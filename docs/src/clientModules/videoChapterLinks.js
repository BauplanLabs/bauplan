/**
 * Plays an embedded video from a timestamped link.
 *
 * When the video is already embedded on the page, 
 * seek that player instead of leaving the docs.
 *
 * Assumes the embed sits above its chapter links, which is what makes this
 * simple: by the time a link is clicked the player has been on screen and has
 * loaded. An embed placed below its links could still be an empty lazy iframe,
 * and would drop the command.
 */

const PLAYER_ORIGIN = "https://www.youtube-nocookie.com";
const CHAPTER_LINK = /^https:\/\/youtu\.be\/([\w-]{11})\?t=(\d+)$/;

function seekEmbeddedPlayer(event) {
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;

  const link = event.target.closest("a[href]");
  if (!link) return;

  const match = CHAPTER_LINK.exec(link.href);
  if (!match) return;

  const [, videoId, seconds] = match;
  const embed = document.querySelector(`[data-bp-video="${videoId}"]`);
  const player = embed?.querySelector("iframe");
  if (!player?.contentWindow) return; // Open YouTube, if there's no embed

  event.preventDefault();

  const box = player.getBoundingClientRect();
  if (box.top < 0 || box.bottom > window.innerHeight) {
    const reduced = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches;
    embed.scrollIntoView({
      behavior: reduced ? "auto" : "smooth",
      block: "center",
    });
  }

  const command = (func, args) =>
    player.contentWindow.postMessage(
      JSON.stringify({ event: "command", func, args }),
      PLAYER_ORIGIN,
    );

  command("seekTo", [Number(seconds), true]);
  command("playVideo", []);
}

if (typeof document !== "undefined") {
  document.addEventListener("click", seekEmbeddedPlayer);
}
