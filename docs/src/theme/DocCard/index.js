import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {
  useDocById,
  findFirstSidebarItemLink,
} from '@docusaurus/plugin-content-docs/client';
import { usePluralForm } from '@docusaurus/theme-common';
import isInternalUrl from '@docusaurus/isInternalUrl';
import { translate } from '@docusaurus/Translate';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

function useCategoryItemsPlural() {
  const { selectMessage } = usePluralForm();
  return (count) =>
    selectMessage(
      count,
      translate(
        {
          message: '1 item|{count} items',
          id: 'theme.docs.DocCard.categoryDescription.plurals',
          description:
            'The default description for a category card in the generated index about how many items this category includes',
        },
        { count },
      ),
    );
}

function CardContainer({ className, href, children }) {
  return (
    <Link
      href={href}
      className={clsx('card padding--lg', styles.cardContainer, className)}>
      {children}
    </Link>
  );
}

// Helper function to check if icon is a custom image
function isCustomImage(icon) {
  if (!icon) return false;

  // Check if it's an imported image (could be string URL or object with default)
  const iconSrc = typeof icon === 'object' ? icon.default : icon;

  // If it's a string that looks like a path/URL (contains / or .)
  if (typeof iconSrc === 'string' && (iconSrc.includes('/') || iconSrc.includes('.'))) {
    return true;
  }

  return false;
}

// Helper function to get the actual src from an icon
function getIconSrc(icon) {
  return typeof icon === 'object' ? icon.default : icon;
}

function CardLayout({ className, href, icon, title, description, isLargeImage }) {
  const hasCustomImage = isCustomImage(icon);
  const iconSrc = hasCustomImage ? getIconSrc(icon) : null;

  return (
    <CardContainer href={href} className={className}>
      <Heading
        as="h2"
        className={clsx('text--truncate', styles.cardTitle)}
        title={title}>
        {hasCustomImage && (
          <div className={clsx(styles.cardIcon, isLargeImage && styles.cardIconLarge)}>
            <img src={iconSrc} alt={title} />
          </div>
        )}

        {!hasCustomImage && icon ? `${icon} ` : null}
        {title}
      </Heading>
      {description && (
        <p
          className={clsx('text--truncate', styles.cardDescription)}
          title={description}>
          {description}
        </p>
      )}
    </CardContainer>
  );
}

function CardCategory({ item }) {
  const href = findFirstSidebarItemLink(item);
  const categoryItemsPlural = useCategoryItemsPlural();
  // Unexpected: categories that don't have a link have been filtered upfront
  if (!href) {
    return null;
  }
  return (
    <CardLayout
      className={item.className}
      href={href}
      icon={item.icon || '🗃️'}
      title={item.label}
      description={item.description ?? categoryItemsPlural(item.items.length)}
    />
  );
}

function CardLink({ item }) {
  const icon = item.icon || item.image || (isInternalUrl(item.href) ? '📄️' : '🔗');
  const doc = useDocById(item.docId ?? undefined);
  // Use image property to determine if it should be larger
  const isLargeImage = !!item.image;
  return (
    <CardLayout
      className={item.className}
      href={item.href}
      icon={icon}
      title={item.label}
      description={item.description ?? doc?.description}
      isLargeImage={isLargeImage}
    />
  );
}

export default function DocCard({ item }) {
  switch (item.type) {
    case 'link':
      return <CardLink item={item} />;
    case 'category':
      return <CardCategory item={item} />;
    default:
      throw new Error(`unknown item type ${JSON.stringify(item)}`);
  }
}
