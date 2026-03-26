import React from 'react';
import clsx from 'clsx';
import { ThemeClassNames, useColorMode } from '@docusaurus/theme-common';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function FooterLayout({ style, links, logo, copyright }) {
  const { siteConfig } = useDocusaurusContext();
  const { colorMode } = useColorMode();
  const socials = siteConfig.customFields?.socials || [];

  return (
    <footer className={clsx(ThemeClassNames.layout.footer.container, 'footer', {
      'footer--dark': style === 'dark',
    })}>
      <div className="container container-fluid">
        {links}
        {(logo || copyright || socials.length > 0) && (
          <div className="flex flex-col gap-4 md:flex-row justify-between items-center">
            {/* Logo */}
            {logo && (
              <div className="flex-shrink-0 [&_img]:h-6">
                {logo}
              </div>
            )}

            {/* Copyright */}
            {copyright && (
              <div className="text-center md:text-left text-sm text-gray-600 dark:text-gray-400">
                {copyright}
              </div>
            )}

            {/* Social Links */}
            {socials.length > 0 && (
              <div className="flex items-center gap-4">
                {socials.map((social, index) => (
                  <a
                    key={index}
                    href={social.href}
                    title={social.title}
                    target="_blank"
                    rel="noreferrer noopener"
                    className="transition-all duration-200 hover:opacity-70 hover:scale-110"
                  >
                    <img
                      src={social.src}
                      alt={social.alt}
                      className={clsx(
                        "w-6 h-6 transition-all duration-200",
                        colorMode === 'light' && social.invertInLight && "invert brightness-75"
                      )}
                    />
                  </a>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </footer>
  );
}
