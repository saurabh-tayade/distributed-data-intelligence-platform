import relativeLinksRule from "markdownlint-rule-relative-links";

export default {
  config: {
    default: true,
    // Existing prose and tables use long lines; let editors wrap them visually.
    MD013: false,
    "relative-links": { root_path: "." },
  },
  globs: ["**/*.md"],
  gitignore: true,
  customRules: [relativeLinksRule],
};
