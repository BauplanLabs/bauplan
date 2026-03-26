import { useState, useMemo } from 'react';
import { useNavbarMobileSidebar } from '@docusaurus/theme-common/internal';
import { useLocation } from '@docusaurus/router';
import Link from '@docusaurus/Link';
import clsx from 'clsx';
import styles from './styles.module.css';

export default function DocSidebarDropdown({ label, items }) {
  const mobileSidebar = useNavbarMobileSidebar();
  const location = useLocation();

  const containsActivePage = useMemo(() => {
    return items.some(item => {
      return location.pathname === item.to ||
        location.pathname === item.to + '/' ||
        location.pathname + '/' === item.to;
    });
  }, [items, location.pathname]);

  const [expanded, setExpanded] = useState(containsActivePage);

  return (
    <li className={styles.dropdown}>
      <button
        type="button"
        className={styles.dropdownButton}
        onClick={() => setExpanded(!expanded)}>
        <span>{label}</span>
        <span className={styles.arrow}>{expanded ? '▲' : '▼'}</span>
      </button>

      {expanded && (
        <ul className={`${styles.dropdownList}  m-0 pl-1`}>
          {items.map((item, i) => {
            const isActive = location.pathname === item.to ||
              location.pathname === item.to + '/' ||
              location.pathname + '/' === item.to;

            return (
              <li key={i} className='ml-4'>
                <Link
                  className={clsx(styles.dropdownLink, {
                    'menu__link--active': isActive,
                  })}
                  to={item.to}
                  onClick={() => mobileSidebar.toggle()}>
                  {item.label}
                </Link>
              </li>
            );
          })}
        </ul>
      )}
    </li>
  );
}
