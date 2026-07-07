import React from "react";

interface RightArrowProps {
  onClick: () => void;
  disabled?: boolean;
  title?: string;
  className?: string;
}

const RightArrow = ({ onClick, disabled, title, className }: RightArrowProps) => {
  return (
    <div
      className={`${className} flex items-center justify-center`}
      onClick={onClick}
      title={title}
    >
      <img
        src="/images/chatbot/Ellipse.png"
        className="absolute w-9 h-9 sm:w-10 sm:h-10  2xl:h-12 2xl:w-12"
        alt="Ellipse"
      />
      <img
        src="/images/chatbot/Plane.png"
        className="relative w-5 h-5 sm:w-5 sm:h-5 md:w-7 md:h-7 lg:w-8 lg:h-8"
        alt="Plane"
      />
    </div>
  );
};

export default RightArrow;
