import React from "react";
import {
  Layers,
  Terminal,
  Package,
  BookOpen,
  GitBranch,
  Workflow,
  Blocks,
  Zap,
} from "lucide-react";
import cards from "./cards.json";

// The card data lives in cards.json so scripts/generate-llm-docs.js can render
// the same links into the markdown version of the landing page.
const icons = {
  Layers,
  Terminal,
  Package,
  BookOpen,
  GitBranch,
  Workflow,
  Blocks,
  Zap,
};

const cardClass = `group flex items-center p-4 rounded-lg
  bg-[var(--ifm-card-background-color)]
  transition-all duration-300 ease-in-out hover:-translate-y-2
  shadow-md
  border border-transparent
  hover:border-[var(--ifm-link-hover-color)] hover:text-[var(--ifm-link-hover-color)]
  no-underline`;

export const HomePage = () => {
  const { sections, agentsCard } = cards;

  return (
    <>
      <a
        href={agentsCard.href}
        className="block no-underline text-[var(--docsearch-text-color)]"
      >
        <div className="my-8 p-6 rounded-xl bg-[var(--ifm-card-background-color)] border border-[var(--ifm-border-color)] hover:scale-[1.01] hover:shadow-md transition-all duration-300 ease-in-out cursor-pointer">
          <div className="text-[var(--docsearch-text-color)]">
            <div className="flex flex-row gap-4">
              <img
                className="w-4 h-8"
                src={agentsCard.image}
                alt={agentsCard.title}
                style={{ transform: "rotate(35deg)" }}
              />
              <h3 className="text-xl font-bold mb-2">{agentsCard.title}</h3>
            </div>
            <p>{agentsCard.description}</p>
          </div>
          <span>{agentsCard.cta}</span>
        </div>
      </a>
      {sections.map((section) => (
        <React.Fragment key={section.title}>
          <h2 className="text-2xl font-bold mb-6">{section.title}</h2>
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
            {section.cards.map((card) => {
              const Icon = icons[card.icon];
              return (
                <a key={card.href} href={card.href} className={cardClass}>
                  <Icon className="h-5 w-5 mr-3" />
                  <div className="text-[var(--docsearch-text-color)]">
                    <div className="font-semibold">{card.title}</div>
                    <div className="text-sm">{card.description}</div>
                  </div>
                </a>
              );
            })}
          </div>
        </React.Fragment>
      ))}
    </>
  );
};
