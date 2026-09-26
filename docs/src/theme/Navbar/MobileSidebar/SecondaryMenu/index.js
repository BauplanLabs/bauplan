import {useNavbarSecondaryMenu} from '@docusaurus/theme-common/internal';

// Stock, minus the "← Back to main menu" button: the docs sidebar menu
// (DocSidebar/Mobile) has its own section switcher.
export default function NavbarMobileSidebarSecondaryMenu() {
  return useNavbarSecondaryMenu().content;
}
