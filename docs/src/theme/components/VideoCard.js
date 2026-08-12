import React, { useCallback, useState } from "react";
import { Play } from "lucide-react";

const PLAYER_ORIGIN = "https://www.youtube-nocookie.com";

const thumbnailUrl = (id, size) => `https://i.ytimg.com/vi/${id}/${size}.jpg`;

export function VideoCardGrid({ children }) {
  return <div className="grid md:grid-cols-3 gap-4 mb-6">{children}</div>;
}

export function VideoCard({ id, title, duration, blurb, start }) {
  const [playing, setPlaying] = useState(false);
  const [thumbnail, setThumbnail] = useState(thumbnailUrl(id, "maxresdefault"));

  const rejectPlaceholder = useCallback(
    (img) => {
      if (img?.complete && img.naturalWidth <= 120) {
        setThumbnail(thumbnailUrl(id, "hqdefault"));
      }
    },
    [id],
  );

  const params = new URLSearchParams({ rel: "0", autoplay: "1" });
  if (start) {
    params.set("start", String(start));
  }

  return (
    <div
      className="flex flex-col overflow-hidden rounded-lg
            bg-[var(--ifm-card-background-color)]
            shadow-md
            border border-transparent
            transition-all duration-300 ease-in-out
            hover:-translate-y-2 hover:border-[var(--ifm-link-hover-color)]"
    >
      <div className="relative aspect-video bg-black">
        {playing ? (
          <iframe
            src={`${PLAYER_ORIGIN}/embed/${id}?${params}`}
            title={title}
            className="absolute inset-0 h-full w-full border-0"
            referrerPolicy="strict-origin-when-cross-origin"
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
          />
        ) : (
          <button
            type="button"
            onClick={() => setPlaying(true)}
            aria-label={`Play video: ${title}`}
            className="group absolute inset-0 h-full w-full cursor-pointer border-0 bg-transparent p-0"
          >
            <img
              ref={rejectPlaceholder}
              src={thumbnail}
              onLoad={(e) => rejectPlaceholder(e.currentTarget)}
              onError={() => setThumbnail(thumbnailUrl(id, "hqdefault"))}
              alt=""
              loading="lazy"
              className="absolute inset-0 h-full w-full object-cover"
            />
            <span className="absolute inset-0 flex items-center justify-center bg-black/25 transition-colors duration-300 group-hover:bg-black/10">
              <span className="flex h-12 w-12 items-center justify-center rounded-full bg-black/70 text-white transition-transform duration-300 group-hover:scale-110">
                <Play className="h-5 w-5 translate-x-px" fill="currentColor" />
              </span>
            </span>
            {duration && (
              <span className="absolute bottom-2 right-2 rounded bg-black/80 px-1.5 py-0.5 font-mono text-xs text-white">
                {duration}
              </span>
            )}
          </button>
        )}
      </div>

      <div className="p-4 text-[var(--docsearch-text-color)]">
        <div className="font-semibold">{title}</div>
        {blurb && <div className="mt-1 text-sm">{blurb}</div>}
      </div>
    </div>
  );
}
