
export default function MutedNote({ text, children }) {
    return (
        <div>
            <span>{text}</span>
            <div className="text-sm text-gray-500 mb-4">
                {children}
            </div>
        </div>
    );
}
