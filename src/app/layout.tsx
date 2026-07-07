// import "./globals.css";
// import { Lato } from "next/font/google";

// import { SidebarProvider } from "@/context/SidebarContext";
// import { ThemeProvider } from "@/context/ThemeContext";
// import Providers from "./providers";
// import { Toaster } from "sonner";

// // 🆕 Replace Outfit with Lato
// const lato = Lato({
//   subsets: ["latin"],
//   weight: ["300", "400", "700", "900"], 
//   variable: "--font-lato",              
// });

// export default function RootLayout({
//   children,
// }: Readonly<{
//   children: React.ReactNode;
// }>) {
//   return (
//     <html lang="en" className={lato.variable}>
//       <body className={`font-sans dark:bg-gray-900`}>
//         <Providers>
//           <ThemeProvider>
//             <SidebarProvider>{children}</SidebarProvider>
//           </ThemeProvider>
//         </Providers>

//         <Toaster position="top-right" richColors closeButton />
//       </body>
//     </html>
//   );
// }

























import "./globals.css";
import { Lato } from "next/font/google";

import { SidebarProvider } from "@/context/SidebarContext";
import { ThemeProvider } from "@/context/ThemeContext";
import Providers from "./providers";
import { Toaster } from "sonner";
import { PlatformProvider } from "@/components/context/PlatformContext";
import { Metadata } from "next";

export const metadata: Metadata = {
  title: {
    default: "Phormula",
    template: "%s | Phormula",
  },
  description: "Phormula dashboard",
  icons: {
    icon: "/favicon.ico",
  },
};

// 🆕 Replace Outfit with Lato
const lato = Lato({
  subsets: ["latin"],
  weight: ["300", "400", "700", "900"],
  variable: "--font-lato",
});

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={lato.variable}>
      <body className="font-sans dark:bg-gray-900">
        <Providers>
          <ThemeProvider>
            <SidebarProvider>
              <PlatformProvider>{children}</PlatformProvider>
            </SidebarProvider>
          </ThemeProvider>
        </Providers>

        <Toaster position="top-right" richColors closeButton />
      </body>
    </html>
  );
}
