import React from "react";

const PLAYER_ORIGIN = "https://www.youtube-nocookie.com";

// `start` is in seconds, for linking straight to a certain point in time.
export function YouTube({ id, title, start }) {
  const params = new URLSearchParams({ rel: "0", enablejsapi: "1" });
  if (start) {
    params.set("start", String(start));
  }

  return (
    <div
      data-bp-video={id}
      style={{
        aspectRatio: "16 / 9",
        maxWidth: "65%",
        marginBottom: "1.5rem",
        border: "1px solid var(--ifm-toc-border-color)",
        borderRadius: "8px",
        overflow: "hidden",
        scrollMarginTop: "5rem",
      }}
    >
      <iframe
        src={`${PLAYER_ORIGIN}/embed/${id}?${params}`}
        title={title || "YouTube video"}
        width="100%"
        height="100%"
        style={{ display: "block", border: "0" }}
        loading="lazy"
        referrerPolicy="strict-origin-when-cross-origin"
        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
        allowFullScreen
      />
    </div>
  );
}
