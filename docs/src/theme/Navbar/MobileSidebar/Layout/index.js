import {useEffect, useRef} from 'react';
import clsx from 'clsx';
import {useNavbarSecondaryMenu, useNavbarMobileSidebar} from '@docusaurus/theme-common/internal';
import {ThemeClassNames} from '@docusaurus/theme-common';
import {useLocation} from '@docusaurus/router';

function NavbarMobileSidebarPanel({children, inert}) {
  return (
    <div
      className={clsx(
        ThemeClassNames.layout.navbar.mobileSidebar.panel,
        'navbar-sidebar__item menu',
      )}
      inert={inert ? true : undefined}>
      {children}
    </div>
  );
}

export default function NavbarMobileSidebarLayout({
  header,
  primaryMenu,
  secondaryMenu,
}) {
  const {shown: secondaryMenuShown, hide} = useNavbarSecondaryMenu();
  const mobileSidebar = useNavbarMobileSidebar();
  const location = useLocation();
  const hasHiddenOnce = useRef(false);

  // Hide secondary menu on root homepage when sidebar first opens
  useEffect(() => {
    if (!mobileSidebar.shown) {
      hasHiddenOnce.current = false;
      return;
    }

    const isRootHomepage = location.pathname === '/';

    if (isRootHomepage && secondaryMenuShown && !hasHiddenOnce.current) {
      hasHiddenOnce.current = true;
      hide();
    }
  }, [mobileSidebar.shown, location.pathname, secondaryMenuShown, hide]);

  return (
    <div
      className={clsx(
        ThemeClassNames.layout.navbar.mobileSidebar.container,
        'navbar-sidebar',
      )}>
      {header}
      <div
        className={clsx('navbar-sidebar__items', {
          'navbar-sidebar__items--show-secondary': secondaryMenuShown,
        })}>
        <NavbarMobileSidebarPanel inert={secondaryMenuShown}>
          {primaryMenu}
        </NavbarMobileSidebarPanel>
        <NavbarMobileSidebarPanel inert={!secondaryMenuShown}>
          {secondaryMenu}
        </NavbarMobileSidebarPanel>
      </div>
    </div>
  );
}
