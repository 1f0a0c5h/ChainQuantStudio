import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
});

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: 'Cha!n｜量化 Agent 工作室',
  description: '遊戲化呈現 Signal 定義、交易策略、回測、訊號與維護 Agent 的量化工作室。',
  openGraph: {
    title: 'Cha!n｜量化 Agent 工作室',
    description: '六位 Agent 在遊戲化工作室中分流 Signal 定義與交易策略研究。',
    locale: 'zh_TW',
    type: 'website',
  },
  twitter: {
    card: 'summary',
    title: 'Cha!n｜量化 Agent 工作室',
    description: '六位 Agent 在遊戲化工作室中分流 Signal 定義與交易策略研究。',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-Hant">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
