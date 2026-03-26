import React from 'react';
import { useColorMode } from '@docusaurus/theme-common';

export function AdvancedBadge() {
  const { colorMode } = useColorMode();
  const style = colorMode === 'dark'
    ? { backgroundColor: '#FFD600', borderColor: '#FFD600', color: '#000' }
    : { backgroundColor: '#00B0FF', borderColor: '#00B0FF', color: '#fff' };
  return <span className="badge" style={style}>Advanced</span>;
}

export function CodeString({ children }) {
  const { colorMode } = useColorMode();
  const color = colorMode === 'dark' ? '#ce9178' : '#a31515';
  return (
    <code style={{ color, background: 'none', border: 'none', padding: 0 }}>
      {children}
    </code>
  );
}
