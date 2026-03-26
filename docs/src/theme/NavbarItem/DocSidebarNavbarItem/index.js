import {
  useActiveDocContext,
  useLayoutDocsSidebar,
} from '@docusaurus/plugin-content-docs/client';
import DefaultNavbarItem from '@theme/NavbarItem/DefaultNavbarItem';
import DocSidebarDropdown from '@theme/NavbarItem/DocSidebarDropdown';
import sidebars from '../../../../sidebar.js';

// Helper to get readable label from doc ID
function getDocLabel(docId) {
  if (!docId) return '';

  const labelMap = {
    'reference/cli': 'CLI Reference',
    'mcp/index': 'MCP Server',
    'mcp/quick_start': 'Quick Start',
    'mcp/advanced_config': 'Advanced Configuration',
    'mcp/tool_list': 'Tool List',
    'faq': 'FAQ',
  };

  return labelMap[docId] || docId;
}

// Convert doc ID to proper URL path
function getDocPath(docId) {
  if (!docId) {
    return '/';
  }
  // Index pages should map to directory path with trailing slash
  if (docId.endsWith('/index')) {
    return `/${docId.replace('/index', '')}/`;
  }
  return `/${docId}`;
}

export default function DocSidebarNavbarItem({
  sidebarId,
  label,
  docsPluginId,
  mobile,
  ...props
}) {
  const {activeDoc} = useActiveDocContext(docsPluginId);
  const sidebarLink = useLayoutDocsSidebar(sidebarId, docsPluginId).link;

  if (!sidebarLink) {
    throw new Error(
      `DocSidebarNavbarItem: Sidebar with ID "${sidebarId}" doesn't have anything to be linked to.`,
    );
  }

  // On mobile, render as expandable dropdown with top-level sections
  if (mobile) {
    const sidebarConfig = sidebars[sidebarId];

    if (!sidebarConfig) {
      return (
        <DefaultNavbarItem
          exact
          {...props}
          isActive={() => activeDoc?.sidebar === sidebarId}
          label={label ?? sidebarLink.label}
          to={sidebarLink.path}
        />
      );
    }

    // Helper to find first non-index item
    const findFirstNonIndexItem = (items) => {
      if (!items || items.length === 0) return null;

      for (const item of items) {
        // Handle nested objects (like categories)
        if (typeof item === 'object' && item !== null) {
          if (item.type === 'category') {
            // Skip nested categories, look in their items
            if (item.items && item.items.length > 0) {
              const nestedDoc = findFirstNonIndexItem(item.items);
              if (nestedDoc) return nestedDoc;
            }
            continue;
          }
          // Regular doc item object
          const docId = item.id;
          if (docId && !docId.endsWith('/index')) {
            return docId;
          }
        } else if (typeof item === 'string') {
          // String doc reference
          if (!item.endsWith('/index')) {
            return item;
          }
        }
      }

      // If all are index pages, return the first non-category string
      for (const item of items) {
        if (typeof item === 'string') {
          return item;
        }
        if (typeof item === 'object' && item !== null && item.id) {
          return item.id;
        }
      }

      return null;
    };

    // For documentationSidebar, skip index pages. For others (examples, integrations), use them.
    const shouldSkipIndexPages = sidebarId === 'documentationSidebar';

    // Convert sidebar categories to dropdown items
    const items = sidebarConfig
      .map((item) => {
        if (item.type === 'category') {
          let docId;

          // If category has a link
          if (item.link && item.link.id) {
            docId = item.link.id;

            // Skip index pages only for documentationSidebar
            if (shouldSkipIndexPages && docId.endsWith('/index') && item.items && item.items.length > 0) {
              const nonIndexItem = findFirstNonIndexItem(item.items);
              if (nonIndexItem) {
                docId = nonIndexItem;
              }
            }
          } else if (item.items && item.items.length > 0) {
            // No explicit link
            if (shouldSkipIndexPages) {
              // For documentationSidebar, skip index pages
              docId = findFirstNonIndexItem(item.items);
            } else {
              // For other sidebars, infer index page from first item
              let firstItem = item.items[0];
              let firstDocId = typeof firstItem === 'string' ? firstItem : firstItem?.id;

              if (firstDocId) {
                const pathParts = firstDocId.split('/');
                if (pathParts.length > 1) {
                  docId = pathParts[0] + '/index';
                } else {
                  docId = firstDocId;
                }
              }
            }
          }

          return {
            label: item.label,
            to: getDocPath(docId),
          };
        }
        // Direct doc items like "faq", "reference/cli", "mcp/index", etc.
        const docId = typeof item === 'string' ? item : item?.id;
        return {
          label: getDocLabel(docId),
          to: getDocPath(docId),
        };
      })
      .filter((item) => {
        // Keep items with valid 'to' properties
        if (!item || !item.to) return false;
        // Keep home item even if it's '/'
        if (item.isHome) return true;
        // Filter out items that defaulted to '/' due to missing docId
        return item.to !== '/';
      });

    // Add "Home" as the first item for Documentation sidebar
    if (sidebarId === 'documentationSidebar') {
      items.unshift({
        label: 'Home',
        to: '/',
        isHome: true,
      });
    }

    return (
      <DocSidebarDropdown
        label={label ?? sidebarLink.label}
        items={items}
      />
    );
  }

  // On desktop, render as normal link
  return (
    <DefaultNavbarItem
      exact
      {...props}
      isActive={() => activeDoc?.sidebar === sidebarId}
      label={label ?? sidebarLink.label}
      to={sidebarLink.path}
    />
  );
}
