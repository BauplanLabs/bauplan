import React from "react";
import Breadcrumbs from "@theme-original/DocBreadcrumbs";
import PageActions from "@site/src/theme/components/PageActions";

export default function DocBreadcrumbsWrapper(props) {
  return (
    <div className="flex items-end justify-between gap-4 mb-[0.8rem] [&_nav]:mb-0">
      <Breadcrumbs {...props} />
      <PageActions />
    </div>
  );
}
