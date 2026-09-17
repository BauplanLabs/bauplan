import React from 'react';
import clsx from 'clsx';
import useIsBrowser from '@docusaurus/useIsBrowser';
import { translate } from '@docusaurus/Translate';
import IconLightMode from '@theme/Icon/LightMode';
import IconDarkMode from '@theme/Icon/DarkMode';
import IconSystemColorMode from '@theme/Icon/SystemColorMode';
import styles from './styles.module.css';

function getNextColorMode(colorMode, respectPrefersColorScheme) {
  if (!respectPrefersColorScheme) {
    return colorMode === 'dark' ? 'light' : 'dark';
  }
  switch (colorMode) {
    case null:
      return 'light';
    case 'light':
      return 'dark';
    case 'dark':
      return null;
    default:
      throw new Error(`unexpected color mode ${colorMode}`);
  }
}

function getColorModeLabel(colorMode) {
  switch (colorMode) {
    case null:
      return translate({
        message: 'system mode',
        id: 'theme.colorToggle.ariaLabel.mode.system',
        description: 'The name for the system color mode',
      });
    case 'light':
      return translate({
        message: 'light mode',
        id: 'theme.colorToggle.ariaLabel.mode.light',
        description: 'The name for the light color mode',
      });
    case 'dark':
      return translate({
        message: 'dark mode',
        id: 'theme.colorToggle.ariaLabel.mode.dark',
        description: 'The name for the dark color mode',
      });
    default:
      throw new Error(`unexpected color mode ${colorMode}`);
  }
}

function getColorModeAriaLabel(colorMode) {
  return translate(
    {
      message: 'Switch between dark and light mode (currently {mode})',
      id: 'theme.colorToggle.ariaLabel',
      description: 'The ARIA label for the color mode toggle',
    },
    { mode: getColorModeLabel(colorMode) },
  );
}

function NextColorModeIcon() {
  // All 3 icons are rendered and CSS picks one, so the right icon is there
  // before React hydrates.
  return (
    <>
      <IconLightMode aria-hidden className={clsx(styles.toggleIcon, styles.lightToggleIcon)} />
      <IconDarkMode aria-hidden className={clsx(styles.toggleIcon, styles.darkToggleIcon)} />
      <IconSystemColorMode aria-hidden className={clsx(styles.toggleIcon, styles.systemToggleIcon)} />
    </>
  );
}

function ColorModeToggle({ className, buttonClassName, respectPrefersColorScheme, value, onChange }) {
  const isBrowser = useIsBrowser();
  const nextColorMode = getNextColorMode(value, respectPrefersColorScheme);
  return (
    <div className={clsx(styles.toggle, className)}>
      <button
        className={clsx(
          'clean-btn',
          styles.toggleButton,
          !isBrowser && styles.toggleButtonDisabled,
          buttonClassName,
        )}
        type="button"
        onClick={() => onChange(nextColorMode)}
        disabled={!isBrowser}
        title={getColorModeLabel(nextColorMode)}
        aria-label={getColorModeAriaLabel(value)}>
        <NextColorModeIcon />
      </button>
    </div>
  );
}

export default React.memo(ColorModeToggle);
