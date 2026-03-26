/**
 * Re-scrolls to the URL hash after cross-page navigation.
 *
 * Docusaurus scrolls to the anchor immediately on route change, but dynamic
 * components (collapsible sections, Markdown rendering) cause layout shifts
 * that move the target element after the initial scroll. This module waits
 * for layout to settle and scrolls again.
 */

export function onRouteDidUpdate({ location }) {
  if (!location.hash) return;

  const id = decodeURIComponent(location.hash.slice(1));

  function scrollToEl() {
    const el = document.getElementById(id);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      return true;
    }
    return false;
  }

  // Wait for layout to settle, then scroll again.
  requestAnimationFrame(() => {
    setTimeout(scrollToEl, 50);
  });
}
