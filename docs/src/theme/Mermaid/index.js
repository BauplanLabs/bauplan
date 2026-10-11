import Mermaid from "@theme-original/Mermaid";
import { useEffect, useRef } from "react";
import { layOutGitGraph } from "@site/src/mermaid/gitGraphLayout";

/**
 * Re-lays out git graph labels once Mermaid has drawn them. Mermaid renders
 * asynchronously and again on every theme switch, so this watches for new SVGs.
 */
export default function MermaidWrapper(props) {
  const ref = useRef(null);

  useEffect(() => {
    const container = ref.current;
    const layOut = () =>
      container.querySelectorAll('svg[aria-roledescription="gitGraph"]').forEach(layOutGitGraph);
    layOut();
    const observer = new MutationObserver(layOut);
    observer.observe(container, { childList: true, subtree: true });
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={ref}>
      <Mermaid {...props} />
    </div>
  );
}
