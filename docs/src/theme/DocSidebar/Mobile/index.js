import {memo, useEffect, useState} from 'react';
import clsx from 'clsx';
import {
  NavbarSecondaryMenuFiller,
  ThemeClassNames,
  useThemeConfig,
} from '@docusaurus/theme-common';
import {useNavbarMobileSidebar} from '@docusaurus/theme-common/internal';
import {
  useDocsSidebar,
  useDocsVersion,
} from '@docusaurus/plugin-content-docs/client';
import DocSidebarItems from '@theme/DocSidebarItems';
import NavbarItem from '@theme/NavbarItem';
import styles from './styles.module.css';

// Mobile menu: a row of the navbar items on top of a docs sidebar. Items
// pointing at a docs sidebar switch the sidebar shown below in place, so
// every section can be browsed without leaving the current page. Other
// items (e.g. the external Examples link) stay plain links.
function DocSidebarMobileSecondaryMenu({path, sidebars, currentSidebar}) {
  const mobileSidebar = useNavbarMobileSidebar();
  const navbarItems = useThemeConfig().navbar.items;
  const [selected, setSelected] = useState(currentSidebar);

  // Start from the current page's sidebar every time the menu opens.
  useEffect(() => {
    if (mobileSidebar.shown) {
      setSelected(currentSidebar);
    }
  }, [mobileSidebar.shown, currentSidebar]);

  return (
    <>
      <ul className={clsx('menu__list', styles.sections)}>
        {navbarItems.map((item, i) =>
          item.type === 'docSidebar' && sidebars[item.sidebarId] ? (
            <li key={i} className="menu__list-item">
              <button
                type="button"
                className={clsx('menu__link', styles.tab, {
                  'menu__link--active': item.sidebarId === selected,
                })}
                onClick={() => setSelected(item.sidebarId)}>
                {item.label}
              </button>
            </li>
          ) : (
            <NavbarItem
              mobile
              {...item}
              onClick={() => mobileSidebar.toggle()}
              key={i}
            />
          ),
        )}
      </ul>
      <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, 'menu__list')}>
        <DocSidebarItems
          key={selected}
          items={sidebars[selected] ?? []}
          activePath={path}
          onItemClick={(item) => {
            // Same as stock: only close the menu when navigating somewhere
            if (item.type === 'link' || (item.type === 'category' && item.href)) {
              mobileSidebar.toggle();
            }
          }}
          level={1}
        />
      </ul>
    </>
  );
}

function DocSidebarMobile({path}) {
  const sidebars = useDocsVersion().docsSidebars;
  const currentSidebar = useDocsSidebar()?.name;
  return (
    <NavbarSecondaryMenuFiller
      component={DocSidebarMobileSecondaryMenu}
      props={{path, sidebars, currentSidebar}}
    />
  );
}

export default memo(DocSidebarMobile);
