import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface PremiumButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  variant?: "primary" | "secondary" | "danger";
}

export function PremiumButton({ 
  children, 
  className, 
  variant = "primary", 
  ...props 
}: PremiumButtonProps) {
  const variants = {
    primary: "bg-gradient-to-r from-crypto-silver to-white text-crypto-black hover:shadow-lg",
    secondary: "bg-gradient-to-r from-crypto-warning to-yellow-400 text-crypto-black hover:shadow-lg",
    danger: "bg-gradient-to-r from-crypto-danger to-red-400 text-white hover:shadow-lg"
  };

  return (
    <Button 
      className={cn(
        "font-semibold transition-all duration-200",
        variants[variant],
        className
      )} 
      {...props}
    >
      {children}
    </Button>
  );
}
