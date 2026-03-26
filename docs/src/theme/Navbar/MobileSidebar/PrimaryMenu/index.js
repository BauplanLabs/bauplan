import {useThemeConfig} from '@docusaurus/theme-common';
import {useNavbarMobileSidebar} from '@docusaurus/theme-common/internal';
import NavbarItem from '@theme/NavbarItem';

function useNavbarItems() {
  return useThemeConfig().navbar.items;
}

export default function NavbarMobilePrimaryMenu() {
  const mobileSidebar = useNavbarMobileSidebar();
  const items = useNavbarItems();

  return (
    <ul className="menu__list">
      {items.map((item, i) => {
        // For docSidebar items, they'll be expandable dropdowns - don't auto-close
        // For regular links, close the sidebar when clicked
        const shouldCloseOnClick = item.type !== 'docSidebar';

        return (
          <NavbarItem
            mobile
            {...item}
            onClick={shouldCloseOnClick ? () => mobileSidebar.toggle() : undefined}
            key={i}
          />
        );
      })}
    </ul>
  );
}
