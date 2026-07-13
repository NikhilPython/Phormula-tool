import React from "react";

interface ServiceCardProps {
  title: string;
  description: string[];
}

const ServiceCard: React.FC<ServiceCardProps> = ({ title, description }) => {
  return (
    <div className="w-[290px] rounded-2xl bg-white/95 p-5 shadow-[0_18px_45px_rgba(55,69,95,0.14)] border border-[#37455F1A]">
      <h2 className="text-[#37455F] text-lg font-extrabold">
        {title}
      </h2>

      <div className="mt-2 h-1 w-full rounded-full bg-[#5EA68E]" />

      <ul className="mt-4 space-y-2 pl-5">
        {description.map((item, index) => (
          <li
            key={index}
            className="text-[15px] leading-snug text-[#414B60] list-disc"
          >
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
};

export default ServiceCard;