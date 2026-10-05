import { useState, memo } from "react";
import clsx from "clsx";
import {
  NavbarSecondaryMenuFiller,
  ThemeClassNames,
  useThemeConfig,
} from "@docusaurus/theme-common";
import { useNavbarMobileSidebar } from "@docusaurus/theme-common/internal";
import {
  useDocsSidebar,
  useDocsVersion,
} from "@docusaurus/plugin-content-docs/client";
import DocSidebarItems from "@theme/DocSidebarItems";
import NavbarItem from "@theme/NavbarItem";

// The navigation doesn't happen until user selects a particular page within a tab
function SectionMenu({ sidebars, currentSidebar, path }) {
  const { items: navbarItems } = useThemeConfig().navbar;
  const mobileSidebar = useNavbarMobileSidebar();
  const [selected, setSelected] = useState(currentSidebar);

  return (
    <>
      <nav className="mobile-section-tabs">
        {navbarItems.map((item, i) =>
          item.type === "docSidebar" ? (
            <button
              type="button"
              key={i}
              className={clsx("clean-btn navbar__item navbar__link", {
                "navbar__link--active": item.sidebarId === selected,
              })}
              onClick={() => setSelected(item.sidebarId)}
            >
              {item.label}
            </button>
          ) : (
            <NavbarItem {...item} key={i} />
          ),
        )}
      </nav>
      <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, "menu__list")}>
        <DocSidebarItems
          items={sidebars[selected]}
          activePath={path}
          onItemClick={(item) => {
            if (item.type === "link" || (item.type === "category" && item.href)) {
              mobileSidebar.toggle();
            }
          }}
          level={1}
        />
      </ul>
    </>
  );
}

// Renders the section menu inside the navbar's mobile menu. The navbar isn't
// wrapped by the docs data provider, so hooks like useDocsVersion() to get the
// sidebar tree don't work here and the sidebars come in as props.
// Keyed so the selected tab resets after navigating.
function SecondaryMenu({ sidebars, currentSidebar, path }) {
  return (
    <SectionMenu
      key={currentSidebar}
      sidebars={sidebars}
      currentSidebar={currentSidebar}
      path={path}
    />
  );
}

function DocSidebarMobile({ path }) {
  const { docsSidebars } = useDocsVersion();
  const currentSidebar = useDocsSidebar()?.name;

  return (
    <NavbarSecondaryMenuFiller
      component={SecondaryMenu}
      props={{ sidebars: docsSidebars, currentSidebar, path }}
    />
  );
}

export default memo(DocSidebarMobile);
