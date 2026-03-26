const ExampleCard = ({
    imageUrl = "",
    title = "",
    description = "",
    href = "#",
    tags = [],
}) => {
    return (
        <a href={href} className="block no-underline h-full">
            <div className="h-full bg-[var(--ifm-card-background-color)]
            border border-transparent hover:border-[var(--ifm-link-hover-color)] rounded-lg shadow-sm
            transition-all duration-300 ease-in-out hover:-translate-y-2 hover:shadow-lg cursor-pointer
            flex flex-col">
                <img
                    className="rounded-t-lg w-full h-56 object-cover flex-shrink-0"
                    src={imageUrl}
                    alt={title || 'Example image'}
                />
                <div className="p-5 flex flex-col flex-grow">
                    <h5 className="mb-2 text-2xl font-bold tracking-tight text-gray-900 dark:text-white">
                        {title}
                    </h5>
                    <p className="mb-3 font-normal text-gray-700 dark:text-gray-400 flex-grow">
                        {description}
                    </p>
                    {tags && tags.length > 0 && (
                        <div className="flex flex-wrap gap-2 mt-auto">
                            {tags.map((tag, index) => (
                                <span
                                    key={index}
                                    className="capitalize inline-flex items-center px-2 py-1 text-md font-medium text-[#00B0FF] bg-blue-50 rounded-lg dark:bg-blue-900 dark:text-blue-300"
                                >
                                    {tag}
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </a>
    );
}

export default ExampleCard;
