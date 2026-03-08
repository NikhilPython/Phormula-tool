"use client";

import { ThemeProvider } from "@/context/ThemeContext";
import Image from "next/image";
import Link from "next/link";
import React from "react";
import { usePathname } from "next/navigation";

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();

  const LeftPanel = () => {
    
    if (pathname.includes("signup")) {
      return (
        <div className=" w-full bg-[#5EA68E] flex flex-col justify-end px-6">
          {/* Logo - top left */}
          <Link href="/" className="fixed left-3 top-3 z-20">
            <Image
              width={220}
              height={40}
              src="/images/auth/Phormula.png"
              alt="Phormula"
              priority
              className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto "
            />
          </Link>

          {/* Centered content */}
          <div className="flex flex-col 2xl:gap-12 gap-10 mt-4 2xl:mt-8 min-[1700px]:mt-10">
            {/* Heading */}
            <h1 className="text-[#414042] text-xl sm:text-xl lg:text-[42px] xl:text-5xl 2xl:text-6xl font-medium leading-tight text-right xl:pr-10">
              <span className="xl:text-nowrap">Trusted Finance Partner </span>
             
             <br className="xl:block hidden"/> for <span className="text-[#F8EDCE]">D2C Entrepreneurs</span>
            </h1>

            {/* Illustration */}
            <div className="  flex items-center justify-center overflow-hidden ">
              <Image
                src="/images/auth/signup.png"
                alt="Trusted Finance partner for D2C Entrepreneur"
                width={700}
                height={500}
                className="object-contain min-[1700px]-w-[800px] min-[1700px]-h-[600px] 2xl:w-[650px] xl:w-[500px] w-[400px]"
                priority
              />
            </div>

          </div>
        </div>
      );
    }

if (
  pathname.startsWith("/signin") ||
  pathname.startsWith("/reset_password") ||   
  pathname.startsWith("/verify-email")
) {
 return (
       <div className="w-full bg-[#5EA68E] flex flex-col justify-end px-6">
          {/* Logo - top left */}
          <Link href="/" className="fixed left-3 top-3 z-20">
            <Image
              width={220}
              height={50}
              src="/images/auth/Phormula.png"
              alt="Phormula"
              priority
              className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto "
            />
          </Link>

          {/* Heading */}
          <div className="flex flex-col min-[1700px]:gap-12 gap-8 mt-8 min-[1700px]:mt-20">
            {/* Heading */}
            <h1 className="text-[#414042] text-xl sm:text-xl lg:text-4xl xl:text-5xl min-[1700px]:text-6xl font-medium leading-tight text-right xl:pr-12">
              Built for Founders,
              <br />
              Powered by <span className="text-[#F8EDCE]">Insight</span>
            </h1>


          {/* Illustration below heading */}
            <div className="  flex items-center justify-center overflow-hidden">
              <Image
                src="/images/auth/signin.png"
                alt="Performance Analysis dashboard"
                 width={700}
                height={500}
                className="object-contain min-[1700px]:w-[800px]  min-[1700px]-h-[600px] 2xl:w-[600px]  xl:w-[450px] w-[400px]"
                priority
              />
            </div>
          </div>
                    </div>
      );
}


    if (pathname.includes("choose-country")) {
     return (
       <div className="w-full bg-[#5EA68E] flex flex-col justify-end px-6">
          {/* Logo - top left */}
          <Link href="/" className="fixed left-3 top-3 z-20">
            <Image
              width={220}
              height={50}
              src="/images/auth/Phormula.png"
              alt="Phormula"
              priority
              className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto "
            />
          </Link>

          {/* Heading */}
          <div className="flex flex-col min-[1700px]:gap-12 gap-10 mt-4 min-[1700px]:mt-10">
            {/* Heading */}
            <h1 className="text-[#414042] text-xl sm:text-xl lg:text-4xl xl:text-5xl min-[1700px]:text-6xl font-medium leading-tight text-right xl:pr-12">
             Control Expenses with
              <br className="" />
              <span className="text-[#F8EDCE] px-1">Auto Marketplace Recons</span>
            </h1>
         

          {/* Illustration below heading */}
         
            <div className="  flex items-center justify-center overflow-hidden">
              <Image
                src="/images/auth/country.png"
                alt="Performance Analysis dashboard"
                width={700}
                height={500}
                 className="object-contain min-[1700px]:w-[800px]  min-[1700px]-h-[600px] 2xl:w-[600px]  xl:w-[450px] w-[400px]"
                priority
              />
            </div>
          </div>
        </div>
      );
    };

    // ---------- Brand ----------
    if (pathname.includes("brand")) {
    return (
       <div className="w-full bg-[#5EA68E] flex flex-col justify-end px-6">
          {/* Logo - top left */}
          <Link href="/" className="fixed left-3 top-3 z-20">
            <Image
              width={220}
              height={50}
              src="/images/auth/Phormula.png"
              alt="Phormula"
              priority
              className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto "
            />
          </Link>

          {/* Heading */}
          <div className="flex flex-col min-[1700px]:gap-12 gap-10 mt-4 min-[1700px]:mt-10">
            {/* Heading */}
            <h1 className="text-[#414042] text-xl sm:text-xl lg:text-4xl xl:text-5xl min-[1700px]:text-6xl font-medium leading-tight text-right pr-12">
             CXO’s tool for&nbsp;<br/> 
              <span className="text-[#F8EDCE]">Faster Decisions</span>
            </h1>
         

          {/* Illustration below heading */}
         
            <div className="  flex items-center justify-center overflow-hidden">
              <Image
                src="/images/auth/brand.png"
                alt="Performance Analysis dashboard"
                 width={700}
                height={500}
                 className="object-contain min-[1700px]:w-[800px]  min-[1700px]-h-[600px] 2xl:w-[600px]  xl:w-[450px] w-[400px]"
                 priority
              />
            </div>
          </div>
        </div>
      );
    };

    // ---------- Revenue ----------
    if (pathname.includes("chooserevenue")) {
       return (
       <div className="w-full bg-[#5EA68E] flex flex-col justify-end px-6">
          {/* Logo - top left */}
          <Link href="/" className="fixed left-3 top-3 z-20">
            <Image
              width={220}
              height={50}
              src="/images/auth/Phormula.png"
              alt="Phormula"
              priority
              className="2xl:w-[220px] 2xl:h-[50px] xl:w-[150px] w-auto "
            />
          </Link>

          {/* Heading */}
          <div className="flex flex-col min-[1700px]:gap-12 gap-10 mt-4 min-[1700px]:mt-10">
            {/* Heading */}
            <h1 className="text-[#414042] text-xl sm:text-xl lg:text-4xl xl:text-5xl min-[1700px]:text-6xl font-medium leading-tight text-right pr-10">
             See. <span className="text-[#F8EDCE]">Forecast. </span>Win.
            </h1>
         
          {/* Illustration below heading */}
            <div className="flex items-center justify-center overflow-hidden">
              <Image
                src="/images/auth/revenue.png"
                alt="Performance Analysis dashboard"
                width={700}
                height={500}
               className="object-contain min-[1700px]:w-[800px]  min-[1700px]-h-[600px] 2xl:w-[600px]  xl:w-[450px] w-[400px]"
                priority
              />
          </div>
           </div>
        </div>
      );
    };
  }

  return (
    <div className="relative p-6 bg-white  sm:p-0">
      <ThemeProvider>
        <div className="relative flex lg:flex-row w-full h-screen justify-center flex-col dark:bg-gray-900 sm:p-0">

          {/* Right Panel */}
          <div className="lg:w-1/2 w-full h-full bg-green-500 dark:bg-white/5 lg:grid items-center hidden">
            <div className="relative items-center justify-center flex z-1">
              <LeftPanel />
            </div>
          </div>

          {children}


        </div>
      </ThemeProvider>
    </div>
  );
}
