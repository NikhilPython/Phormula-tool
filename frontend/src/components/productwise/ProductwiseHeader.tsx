// "use client";

// import React from "react";
// import PageBreadcrumb from "../common/PageBreadCrumb";

// interface ProductwiseHeaderProps {
//   canShowResults: boolean;
//   countryName?: string;
//   productname: string;
//   headingPeriod: string;
// }

// const ProductwiseHeader: React.FC<ProductwiseHeaderProps> = ({
//   canShowResults,
//   countryName,
//   productname,
//   headingPeriod,
// }) => {
//   return (
//     <div className="mb-6">
//       {/* Breadcrumb */}
//       <PageBreadcrumb
//         pageTitle="SKU Performance Analysis"
//         variant="page"
//         align="left"
//         textSize="2xl"
//       />
//       {/* 🔽 Subtitle moved here (was earlier inside TrendChartSection) */}
//       <p className="mt-1 text-xs sm:text-sm text-charcoal-500">
//         Yearly performance comparison across regions
//       </p>


//     </div>
//   );
// };

// export default ProductwiseHeader;












"use client";

import React from "react";
import PageBreadcrumb from "../common/PageBreadCrumb";

interface ProductwiseHeaderProps {
  canShowResults: boolean;
  countryName?: string;
  productname: string;
  headingPeriod: string;
}

const ProductwiseHeader: React.FC<ProductwiseHeaderProps> = ({
  canShowResults,
  countryName,
  productname,
  headingPeriod,
}) => {
  return (
    <div className="flex flex-col">
      <PageBreadcrumb
        pageTitle="SKU Performance Analysis"
        variant="page"
        align="left"
        textSize="2xl"
      />
      <p className="mt-1 text-xs sm:text-sm text-charcoal-500">
        Yearly performance comparison across regions
      </p>
    </div>
  );
};

export default ProductwiseHeader;
