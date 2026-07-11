declare module "qrcode" {
  const QRCode: {
    toDataURL(text: string, options?: Record<string, unknown>): Promise<string>
    toCanvas(canvas: HTMLCanvasElement, text: string, options?: Record<string, unknown>): Promise<void>
  }

  export default QRCode
}
