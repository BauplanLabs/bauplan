import { Layers, Terminal, Package } from 'lucide-react';

export const HomePage = () => {
    return (
        <>
            <h2 className="text-2xl font-bold mb-6">Get Started</h2>
            <div className="grid md:grid-cols-3 gap-4 mb-8">
                <a href="/tutorial/installation" className="group flex items-center p-4 rounded-lg
                bg-[var(--ifm-card-background-color)]
                transition-all duration-300 ease-in-out hover:-translate-y-2
                shadow-md
                border border-transparent
                hover:border-[var(--ifm-link-hover-color)] hover:text-[var(--ifm-link-hover-color)]
                no-underline">
                    <Package className="h-5 w-5 mr-3" />
                    <div className='text-[var(--docsearch-text-color)]'>
                        <div className="font-semibold">Setup</div>
                        <div className="text-sm ">Quick setup guide</div>
                    </div>
                </a>
                <a href="https://github.com/BauplanLabs/examples" className="group flex items-center p-4 rounded-lg
                bg-[var(--ifm-card-background-color)]
                transition-all duration-300 ease-in-out hover:-translate-y-2
                shadow-md
                border border-transparent
                hover:border-[var(--ifm-link-hover-color)] hover:text-[var(--ifm-link-hover-color)]
                no-underline">
                    <Layers className="h-5 w-5 mr-3" />
                    <div className='text-[var(--docsearch-text-color)]'>
                        <div className="font-semibold">Examples</div>
                        <div className="text-sm ">Learn by example</div>
                    </div>
                </a>

                <a href="/reference/bauplan" className="group flex items-center p-4 rounded-lg
                bg-[var(--ifm-card-background-color)]
                transition-all duration-300 ease-in-out hover:-translate-y-2
                shadow-md
                border border-transparent
                hover:border-[var(--ifm-link-hover-color)] hover:text-[var(--ifm-link-hover-color)]
                no-underline">
                    <Terminal className="h-5 w-5 mr-3" />
                    <div className='text-[var(--docsearch-text-color)]'>
                        <div className="font-semibold">API Reference</div>
                        <div className="text-sm ">Complete API docs</div>
                    </div>
                </a>

            </div>

            <a href="/agents" className="block no-underline text-[var(--docsearch-text-color)]">
                <div className="my-8 p-6 rounded-xl bg-[var(--ifm-card-background-color)] border border-[var(--ifm-border-color)] hover:scale-[1.01] hover:shadow-md transition-all duration-300 ease-in-out cursor-pointer">
                    <div className='text-[var(--docsearch-text-color)]'>
                        <div className='flex flex-row gap-4'>
                            <img className='w-4 h-8'
                                src="/img/icons/rocket.png"
                                alt="Rocket"
                                style={{ transform: 'rotate(35deg)' }}
                            />
                            <h3 className="text-xl font-bold mb-2">AI Agents</h3>
                        </div>
                        <p>Use AI Agents to build pipelines, explore data, and manage your lakehouse with MCP Server and Skills.</p>
                    </div>
                    <span>Learn more →</span>
                </div>
            </a>
        </>
    );
}
