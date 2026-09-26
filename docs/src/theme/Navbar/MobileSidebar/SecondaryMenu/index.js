import clsx from 'clsx';
import {useThemeConfig} from '@docusaurus/theme-common';
import {
  useNavbarMobileSidebar,
  useNavbarSecondaryMenu,
} from '@docusaurus/theme-common/internal';
import NavbarItem from '@theme/NavbarItem';
import styles from './styles.module.css';

// Replaces the stock "← Back to main menu" button with a row of the navbar
// items, so every page's mobile menu shows the section switcher on top of
// the current section's sidebar.
export default function NavbarMobileSidebarSecondaryMenu() {
  const items = useThemeConfig().navbar.items;
  const mobileSidebar = useNavbarMobileSidebar();
  const secondaryMenu = useNavbarSecondaryMenu();

  return (
    <>
      {items.length > 0 && (
        <ul className={clsx('menu__list', styles.sections)}>
          {items.map((item, i) => (
            <NavbarItem
              mobile
              {...item}
              onClick={() => mobileSidebar.toggle()}
              key={i}
            />
          ))}
        </ul>
      )}
      {secondaryMenu.content}
    </>
  );
}
