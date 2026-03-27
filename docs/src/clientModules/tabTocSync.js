/**
 * Syncs the right-side Table of Contents with the active Docusaurus tab.
 *
 * When a page uses <Tabs> with headings inside each <TabItem>, Docusaurus
 * generates TOC entries for ALL headings across all tabs. This module hides
 * TOC entries whose target headings are inside inactive (hidden) tab panels.
 */

function syncToc() {
  const tocLinks = document.querySelectorAll(".table-of-contents a");
  if (!tocLinks.length) return;

  for (const link of tocLinks) {
    const href = link.getAttribute("href");
    if (!href || !href.startsWith("#")) continue;

    const id = decodeURIComponent(href.slice(1));
    const heading = document.getElementById(id);
    if (!heading) continue;

    // Check if this heading is inside a hidden TabItem
    const tabPanel = heading.closest("[role='tabpanel']");
    const listItem = link.closest("li");
    if (!listItem) continue;

    if (tabPanel && tabPanel.hidden) {
      listItem.style.display = "none";
    } else {
      listItem.style.display = "";
    }
  }
}

export function onRouteDidUpdate() {
  // Run after DOM settles (tabs render asynchronously)
  requestAnimationFrame(() => {
    setTimeout(() => {
      syncToc();
      // Observe tab changes via clicks on tab buttons
      observeTabClicks();
    }, 100);
  });
}

let observing = false;

function observeTabClicks() {
  if (observing) return;
  observing = true;

  // Listen for tab clicks anywhere on the page
  document.addEventListener("click", (e) => {
    const tab = e.target.closest("[role='tab']");
    if (tab) {
      // Small delay to let Docusaurus update the active tab panel
      requestAnimationFrame(() => {
        setTimeout(syncToc, 50);
      });
    }
  });

  // Also observe DOM mutations for tab panel visibility changes
  const observer = new MutationObserver(() => {
    syncToc();
  });

  const tabPanels = document.querySelectorAll("[role='tabpanel']");
  for (const panel of tabPanels) {
    observer.observe(panel, { attributes: true, attributeFilter: ["hidden"] });
  }
}
