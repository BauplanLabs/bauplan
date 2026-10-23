import React from "react";
import Breadcrumbs from "@theme-original/DocBreadcrumbs";
import { useLocation } from "@docusaurus/router";
import useBaseUrl from "@docusaurus/useBaseUrl";
import PageActions from "@site/src/theme/components/PageActions";

export default function DocBreadcrumbsWrapper(props) {
  const { pathname } = useLocation();
  const homePath = useBaseUrl("/");

  if (pathname === homePath) return <Breadcrumbs {...props} />;

  return (
    <div className="flex items-end justify-between gap-4 mb-[0.8rem] [&_nav]:mb-0">
      <Breadcrumbs {...props} />
      <PageActions />
    </div>
  );
}
