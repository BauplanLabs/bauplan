import React, { createContext, useContext, useRef, useState } from "react";
import CodeBlockContainer from "@theme/CodeBlock/Container";
import { Copy, Check } from "lucide-react";
import { useSignatureStyles, AnnotationTokens, BUILTIN_TYPES, classifyToken } from "./AnnotationTokenizer";

const SignatureStylesContext = createContext(null);

export function PySignature({ name, children, returns }) {
  const styles = useSignatureStyles();
  const codeRef = useRef(null);
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    const text = codeRef.current?.innerText ?? '';
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <SignatureStylesContext.Provider value={styles}>
      <div className="group" style={{ position: 'relative' }}>
        <CodeBlockContainer as="pre" tabIndex={0} className="thin-scrollbar">
          <code ref={codeRef} style={{
            font: 'inherit',
            float: 'left',
            minWidth: '100%',
          }}>
            <span style={{ color: styles.plain.color }}>{name}</span>
            <span style={styles.punctuation}>(</span>
            {React.Children.count(children) > 0 && '\n'}
            {children}
            <span style={styles.punctuation}>)</span>
            {returns && (
              <>
                <span style={styles.operator}>{' -> '}</span>
                <AnnotationTokens text={returns} styles={styles} />
              </>
            )}
            {returns && <span style={styles.punctuation}>{': ...'}</span>}
            {'\n'}
          </code>
        </CodeBlockContainer>
        <button
          onClick={handleCopy}
          title={copied ? 'Copied!' : 'Copy'}
          style={{ position: 'absolute', top: '0.5rem', right: '0.5rem' }}
          className="clean-btn opacity-0 group-hover:opacity-100 transition-opacity duration-150 text-gray-400 hover:text-gray-200 p-1 rounded"
        >
          {copied ? <Check size={16} /> : <Copy size={16} />}
        </button>
      </div>
    </SignatureStylesContext.Provider>
  );
}

export function PySignatureParam({ name, annotation, defaultValue, separator = "," }) {
  const styles = useContext(SignatureStylesContext);

  // The * keyword-only marker
  if (name === '*') {
    return (
      <span>
        {'    '}<span style={styles.operator}>*</span>{'\n'}
      </span>
    );
  }

  return (
    <span>
      {'    '}<span style={{ color: styles.plain.color }}>{name}</span>
      {annotation && (
        <>
          <span style={styles.punctuation}>: </span>
          <AnnotationTokens text={annotation} styles={styles} />
        </>
      )}
      {defaultValue && (
        <>
          <span style={styles.operator}> = </span>
          <span style={classifyToken(defaultValue, styles)}>{defaultValue}</span>
        </>
      )}
      {separator && <span style={styles.punctuation}>{separator}</span>}{'\n'}
    </span>
  );
}
