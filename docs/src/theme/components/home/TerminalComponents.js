export const Terminal = ({ children }) => {
  return (
    <div className="w-full max-w-6xl bg-[#262626] rounded-xl overflow-hidden">
      {/* header */}
      <div className="bg-zinc-700 px-5 py-3 flex items-center gap-2">
        {/* buttons */}
        <div className="flex gap-2">
          <div className="w-3 h-3 bg-[#FF4C61] rounded-full"></div>
          <div className="w-3 h-3 bg-[#00B0FF] rounded-full"></div>
          <div className="w-3 h-3 bg-[#FFD600] rounded-full"></div>
        </div>
      </div>

      {/* content */}
      <div style={{ fontFamily: 'var(--ifm-font-family-monospace)' }} className="p-10 min-h-[400px]" >
        {children}
      </div>
    </div>
  );
};

export const BauplanAsciiArt = () => {
  return (
    <pre className="px-0 text-[#00B0FF] bg-[#262626] font-bold leading-tight mb-10 tracking-tight whitespace-pre text-[6px] sm:text-xs">
      {`██████╗  █████╗ ██╗   ██╗██████╗ ██╗      █████╗ ███╗   ██╗
██╔══██╗██╔══██╗██║   ██║██╔══██╗██║     ██╔══██╗████╗  ██║
██████╔╝███████║██║   ██║██████╔╝██║     ███████║██╔██╗ ██║
██╔══██╗██╔══██║██║   ██║██╔═══╝ ██║     ██╔══██║██║╚██╗██║
██████╔╝██║  ██║╚██████╔╝██║     ███████╗██║  ██║██║ ╚████║
╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝`}
    </pre>
  );
};

export const TerminalLine = ({
  prompt = "$",
  command,
  output,
  showCursor = false,
  animated = true
}) => {
  const baseClasses = animated ? "animate-fade-in" : "";

  return (
    <div className={`mb-5 text-base ${baseClasses}`}>
      {command && (
        <div className="flex items-center">
          <span className="text-[#FFD600] mr-4">{prompt}</span>
          <span className="text-white font-semibold">{command}</span>
          {showCursor && (
            <span className="inline-block w-2.5 h-5 bg-[#00B0FF] ml-1 animate-pulse"></span>
          )}
        </div>
      )}
      {output && (
        <div className="text-green-400 whitespace-pre-line mt-1 ">
          {output}
        </div>
      )}
    </div>
  );
};

export const SectionTitle = ({ children }) => {
  return (
    <div style={{ fontFamily: 'var(--ifm-font-family-monospace)' }} className="text-gray-400 text-sm uppercase tracking-[2px] mt-8 mb-5">
      {children}
    </div>
  );
};

export const Description = ({ children }) => {
  return (
    <div className="hidden sm:block text-gray-300 text-base mb-10 leading-relaxed">
      {children}
    </div>
  );
};
