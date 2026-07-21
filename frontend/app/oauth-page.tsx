import { useEffect, useState } from "react"
import { useSearchParams } from "react-router-dom"
import { Bot, ShieldCheck, Sparkles } from "lucide-react"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { PageHeader } from "@/components/page-header"
import { OAuthPoolManager } from "@/components/oauth/oauth-pool-manager"
import type { OAuthPoolKind, OAuthPoolQuery } from "@/components/oauth/types"

const DEFAULT_QUERY: OAuthPoolQuery = {
  status: "all",
  page: 1,
  pageSize: 10,
}

export default function OAuthPage() {
	const [searchParams, setSearchParams] = useSearchParams()
	const requestedPool = searchParams.get("pool") === "grok" ? "grok" : "chatgpt"
	const [pool, setPool] = useState<OAuthPoolKind>(requestedPool)
  const [queries, setQueries] = useState<Record<OAuthPoolKind, OAuthPoolQuery>>({
    chatgpt: { ...DEFAULT_QUERY },
    grok: { ...DEFAULT_QUERY },
  })
	useEffect(() => setPool(requestedPool), [requestedPool])

  return (
    <section className="space-y-5">
      <PageHeader
        icon={<ShieldCheck className="size-[18px]" />}
        title="OAuth 登录与账号管理"
        description="通过 JSON 导入并管理 ChatGPT、Grok 号池账号；轮询资格完全以后端实际测活状态为准。"
        meta={<span className="inline-flex items-center gap-1.5 rounded-md border border-border bg-background px-2 py-1 text-[11px] text-muted-foreground"><Sparkles className="size-3 text-brand" />凭据脱敏</span>}
      />

      <Tabs value={pool} onValueChange={(value) => setPool(value as OAuthPoolKind)}>
        <TabsList className="h-10 w-full sm:w-auto">
          <TabsTrigger value="chatgpt" className="flex-1 px-4 sm:flex-none">
            <Bot />
            ChatGPT 号池
          </TabsTrigger>
          <TabsTrigger value="grok" className="flex-1 px-4 sm:flex-none">
            <Sparkles />
            Grok 号池
          </TabsTrigger>
        </TabsList>
      </Tabs>

      <OAuthPoolManager
        key={pool}
        pool={pool}
        query={queries[pool]}
        onQueryChange={(next) => {
          setQueries((current) => ({ ...current, [pool]: next }))
        }}
			importRequested={searchParams.get("import") === "1"}
			onImportRequestHandled={() => {
				const next = new URLSearchParams(searchParams)
				next.delete("import")
				setSearchParams(next, { replace: true })
			}}
      />
    </section>
  )
}
