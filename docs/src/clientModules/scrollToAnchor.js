/**
 * Scrolls to URL hash targets after cross-page navigation.
 *
 * Supports two hash formats:
 *   #heading-id          — scrolls to the heading (standard behavior)
 *   #group-id=tab-value  — scrolls to a tab group and selects the tab
 *
 * Tab groups use <div id="group-id"> around <Tabs> in MDX. Clicking a tab
 * updates the hash; navigating to a hash selects the tab and scrolls.
 */

export function onRouteDidUpdate({ location }) {
  if (!location.hash) return;

  const raw = decodeURIComponent(location.hash.slice(1));
  const eqIndex = raw.indexOf('=');

  if (eqIndex !== -1) {
    const groupId = raw.slice(0, eqIndex);
    const tabValue = raw.slice(eqIndex + 1);
    scrollToTab(groupId, tabValue);
  } else {
    scrollToEl(raw);
  }
}

function scrollToEl(id) {
  requestAnimationFrame(() => {
    setTimeout(() => {
      const el = document.getElementById(id);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }, 50);
  });
}

function slugify(text) {
  return text.trim().toLowerCase().replace(/\s+/g, '-');
}

function scrollToTab(groupId, tabValue) {
  requestAnimationFrame(() => {
    setTimeout(() => {
      const container = document.getElementById(groupId);
      if (!container) return;

      // Find and click the matching tab button
      const tabs = container.querySelectorAll("[role='tab']");
      for (const tab of tabs) {
        if (slugify(tab.textContent) === tabValue) {
          tab.click();
          break;
        }
      }

      container.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 100);
  });
}

/**
 * Updates the URL hash when a tab inside a div[id] wrapper is clicked.
 */
let listening = false;

export function onRouteUpdate() {
  if (listening) return;
  listening = true;

  document.addEventListener('click', (e) => {
    const tab = e.target.closest("[role='tab']");
    if (!tab) return;

    const wrapper = tab.closest('div[id]');
    if (!wrapper) return;

    // Only act on wrappers that directly contain a .tabs-container
    const tabsContainer = wrapper.querySelector('.tabs-container');
    if (!tabsContainer) return;

    // Get the tab value from text content (Docusaurus doesn't expose value in DOM)
    const tabList = tab.closest("[role='tablist']");
    if (!tabList) return;

    // Find the TabItem value: match by position with the Tabs children
    // Docusaurus tab text = label ?? value, so we use text as the value
    const groupId = wrapper.id;
    const hash = `${groupId}=${encodeURIComponent(slugify(tab.textContent))}`;

    history.replaceState(null, '', `#${hash}`);
  });
}
