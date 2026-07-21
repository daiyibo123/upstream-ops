export const PROJECT_REPOSITORY_URL = "https://github.com/daiyibo123/upstream-ops"

export function projectReleaseURL(version?: string | null) {
  const tag = version?.trim()
  if (!tag) return `${PROJECT_REPOSITORY_URL}/releases`
  return `${PROJECT_REPOSITORY_URL}/releases/tag/${tag.toLowerCase().startsWith("v") ? tag : `v${tag}`}`
}
