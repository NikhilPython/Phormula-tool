"use client";

import React from "react";
import PageBreadcrumb from "../common/PageBreadCrumb";

type Props = {
  loading: boolean;
  error?: string | null;
  recommendationBullets?: string[];
  inventoryBullets?: string[];
};

const RecommendationsCard: React.FC<Props> = ({
  loading,
  error,
  recommendationBullets = [],
  inventoryBullets = [],
}) => {
  return (
    <div className="relative w-full rounded-xl border border-slate-200 bg-white shadow-sm p-4 sm:p-5">
      <PageBreadcrumb
        pageTitle="Recommendations"
        variant="page"
        align="left"
        textSize="2xl"
        className="mb-4"
      />

      {loading ? (
        <div className="text-xs 2xl:text-sm text-charcoal-500">Loading…</div>
      ) : error ? (
        <div className="text-xs 2xl:text-sm text-red-600">{error}</div>
      ) : recommendationBullets.length ? (
        <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500">
          {recommendationBullets.map((b, i) => (
            <li key={i}>{b}</li>
          ))}
        </ul>
      ) : (
        <div className="text-xs 2xl:text-sm text-charcoal-500">
          Recommendations are generated only for the latest completed period.
        </div>
      )}

      {inventoryBullets.length ? (
        <>
          <div className="mt-4 text-xs 2xl:text-lg font-semibold text-charcoal-500">
            Inventory
          </div>
          <ul className="list-disc pl-5 space-y-1 text-xs 2xl:text-sm text-charcoal-500 mt-2">
            {inventoryBullets.map((b, i) => (
              <li key={i}>{b}</li>
            ))}
          </ul>
        </>
      ) : null}
    </div>
  );
};

export default RecommendationsCard;
