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
      className={clsx('card padding--md', styles.cardContainer, className)}>
      {children}
    </Link>
  );
}

function isCustomImage(icon) {
  if (!icon) return false;
  const iconSrc = typeof icon === 'object' ? icon.default : icon;
  if (typeof iconSrc === 'string' && (iconSrc.includes('/') || iconSrc.includes('.'))) {
    return true;
  }
  return false;
}

function getIconSrc(icon) {
  return typeof icon === 'object' ? icon.default : icon;
}

function CardLayout({ className, href, icon, title, description, isLargeImage, comingSoon }) {
  const hasCustomImage = isCustomImage(icon);
  const iconSrc = hasCustomImage ? getIconSrc(icon) : null;
  const isReactIcon = React.isValidElement(icon);
  const hasRichIcon = isReactIcon || hasCustomImage;

  if (hasRichIcon) {
    return (
      <CardContainer href={href} className={clsx(className, styles.cardTwoColumn)}>
        <div className={styles.cardTwoColumnIcon}>
          {hasCustomImage && (
            <div className={clsx(styles.cardIcon, isLargeImage && styles.cardIconLarge)}>
              <img src={iconSrc} alt={title} />
            </div>
          )}
          {isReactIcon && (
            <div className={styles.cardLucideIcon}>
              {icon}
            </div>
          )}
        </div>
        <div className={styles.cardTwoColumnContent}>
          <Heading
            as="h2"
            className={clsx('text--truncate', styles.cardTitle)}
            title={title}>
            {title}
            {comingSoon && <span className={styles.comingSoonBadge}>Coming soon</span>}
          </Heading>
          {description && (
            <p
              className={clsx('text--truncate', styles.cardDescription)}
              {...(typeof description === 'string' ? { title: description } : {})}>
              {description}
            </p>
          )}
        </div>
      </CardContainer>
    );
  }

  return (
    <CardContainer href={href} className={className}>
      <Heading
        as="h2"
        className={clsx('text--truncate', styles.cardTitle)}
        title={title}>
        {icon ? `${icon} ` : null}
        {title}
        {comingSoon && <span className={styles.comingSoonBadge}>Coming soon</span>}
      </Heading>
      {description && (
        <p
          className={clsx('text--truncate', styles.cardDescription)}
          {...(typeof description === 'string' ? { title: description } : {})}>
          {description}
        </p>
      )}
    </CardContainer>
  );
}

function CardCategory({ item }) {
  const href = findFirstSidebarItemLink(item);
  const categoryItemsPlural = useCategoryItemsPlural();
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
  const fallback = item.icon === null ? null : (isInternalUrl(item.href) ? '📄️' : '🔗');
  const icon = item.icon || item.image || fallback;
  const doc = useDocById(item.docId ?? undefined);
  const isLargeImage = !!item.image;
  return (
    <CardLayout
      className={item.className}
      href={item.href}
      icon={icon}
      title={item.label}
      description={item.description ?? doc?.description}
      isLargeImage={isLargeImage}
      comingSoon={item.comingSoon}
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
