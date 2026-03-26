export const ConceptCards = ({ items }) => {
    return (
        <div className="grid md:grid-cols-3 gap-4 my-6">
            {items.map((item, index) => (
                <a
                    key={index}
                    href={item.href}
                    className="group flex items-center p-4 rounded-lg
                    bg-[var(--ifm-card-background-color)]
                    transition-all duration-300 ease-in-out hover:-translate-y-2
                    shadow-md
                    border border-transparent
                    hover:border-[var(--ifm-link-hover-color)] hover:text-[var(--ifm-link-hover-color)]
                    no-underline"
                >
                    {item.icon && (
                        <item.icon className="h-5 w-5 mr-3 flex-shrink-0" />
                    )}
                    <div className='text-[var(--docsearch-text-color)]'>
                        <div className="font-semibold">{item.title}</div>
                        {item.description && (
                            <div className="text-sm">{item.description}</div>
                        )}
                    </div>
                </a>
            ))}
        </div>
    );
};
