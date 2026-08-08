import React from "react";
import MDXComponents from "@theme-original/MDXComponents";
import * as Admonitions from "./components/Admonitions";
import * as PyReference from "./components/PyReference";
import * as CliReference from "./components/CliReference";
import * as Badges from "./components/Badges";
import { YouTube } from "./components/YouTube";
import { VideoCard, VideoCardGrid } from "./components/VideoCard";

export default {
  ...MDXComponents,
  ...Admonitions,
  ...PyReference,
  ...CliReference,
  ...Badges,
  YouTube,
  VideoCard,
  VideoCardGrid,
};
