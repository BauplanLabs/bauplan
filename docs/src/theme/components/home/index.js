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
import { HeroDiagram } from "./HeroDiagram";

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

const cardClass = `group flex items-start p-6 rounded-lg
  bg-[var(--ifm-card-background-color)]
  transition-all duration-300 ease-in-out hover:-translate-y-2
  shadow-md
  border border-transparent
  hover:border-[var(--ifm-link-hover-color)] hover:text-[var(--ifm-link-hover-color)]
  no-underline`;

export const HeroIntro = ({ children }) => (
  <div className="lg:grid lg:grid-cols-[minmax(0,34rem)_minmax(320px,1fr)] lg:gap-16 lg:items-center">
    <div>{children}</div>
    <div className="hidden lg:block w-full max-w-[460px] justify-self-center">
      <HeroDiagram />
    </div>
  </div>
);

export const HomePage = () => {
  const { sections, agentsCard } = cards;

  return (
    <>
      <a
        href={agentsCard.href}
        className="group block no-underline text-[var(--docsearch-text-color)]"
      >
        <div className="my-16 p-8 rounded-xl bg-[var(--ifm-card-background-color)] border border-[var(--ifm-border-color)] hover:scale-[1.01] hover:shadow-md transition-all duration-300 ease-in-out cursor-pointer flex items-center justify-between gap-8">
          <div>
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
        </div>
      </a>
      {sections.map((section) => (
        <React.Fragment key={section.title}>
          <h2 className="text-2xl font-bold mb-6">{section.title}</h2>
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
            {section.cards.map((card) => {
              const Icon = icons[card.icon];
              return (
                <a key={card.href} href={card.href} className={cardClass}>
                  <Icon className="h-5 w-5 mr-4 mt-0.5 shrink-0" />
                  <div className="text-[var(--docsearch-text-color)]">
                    <div className="font-semibold">{card.title}</div>
                    <div className="text-sm mt-1">{card.description}</div>
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
