
/opt/transparent-publisher-build/shoup-subtle-20260911/artifacts/worker-integration-tests:     file format elf64-x86-64


Disassembly of section .text:

00000000006503e0 <valar_spiral_rs::ntt::avx2::ntt_forward>:
  6503e0:	push   %rbp
  6503e1:	push   %r15
  6503e3:	push   %r14
  6503e5:	push   %r13
  6503e7:	push   %r12
  6503e9:	push   %rbx
  6503ea:	sub    $0xe8,%rsp
  6503f1:	mov    %rsi,0x8(%rsp)
  6503f6:	mov    0x40(%rdi),%r9
  6503fa:	cmp    $0x1,%r9
  6503fe:	jne    650803 <valar_spiral_rs::ntt::avx2::ntt_forward+0x423>
  650404:	mov    0x38(%rdi),%r8
  650408:	mov    %r8d,%ecx
  65040b:	and    $0x3f,%ecx
  65040e:	mov    $0x1,%eax
  650413:	mov    %rcx,0xb0(%rsp)
  65041b:	shlx   %rcx,%rax,%r9
  650420:	mov    0x8(%rdi),%rcx
  650424:	mov    0x10(%rdi),%rsi
  650428:	xor    %eax,%eax
  65042a:	mov    %r8,0x78(%rsp)
  65042f:	test   %r8,%r8
  650432:	setne  %r8b
  650436:	mov    %r9,0x28(%rsp)
  65043b:	je     650f28 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb48>
  650441:	cmp    %rdx,%r9
  650444:	ja     651172 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd92>
  65044a:	test   %rsi,%rsi
  65044d:	je     65123d <valar_spiral_rs::ntt::avx2::ntt_forward+0xe5d>
  650453:	mov    0x10(%rcx),%rdx
  650457:	test   %rdx,%rdx
  65045a:	je     6511e3 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe03>
  650460:	cmp    $0x1,%rdx
  650464:	je     6511f6 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe16>
  65046a:	mov    %r8b,%al
  65046d:	mov    0x8(%rcx),%rcx
  650471:	mov    0x8(%rcx),%rdx
  650475:	mov    %rdx,0x50(%rsp)
  65047a:	mov    0x10(%rcx),%rdx
  65047e:	mov    %rdx,0x18(%rsp)
  650483:	mov    0x20(%rcx),%rdx
  650487:	mov    %rdx,0x48(%rsp)
  65048c:	mov    0x28(%rcx),%rcx
  650490:	mov    %rcx,0x30(%rsp)
  650495:	mov    0xa8(%rdi),%rcx
  65049c:	mov    %rcx,0x40(%rsp)
  6504a1:	add    %rcx,%rcx
  6504a4:	mov    %rcx,0x20(%rsp)
  6504a9:	xor    %ecx,%ecx
  6504ab:	mov    $0x1,%edx
  6504b0:	mov    0x40(%rsp),%rbx
  6504b5:	jmp    6504dd <valar_spiral_rs::ntt::avx2::ntt_forward+0xfd>
  6504b7:	nopw   0x0(%rax,%rax,1)
  6504c0:	mov    0x80(%rsp),%rsi
  6504c8:	lea    0x1(%rsi),%rax
  6504cc:	mov    %rax,%rdx
  6504cf:	mov    %rsi,%rcx
  6504d2:	cmp    0x78(%rsp),%rsi
  6504d7:	je     650ec0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xae0>
  6504dd:	mov    %rax,%rdi
  6504e0:	mov    0x28(%rsp),%rax
  6504e5:	shrx   %rdx,%rax,%r9
  6504ea:	mov    %r9,%rsi
  6504ed:	add    %r9,%rsi
  6504f0:	je     65113b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd5b>
  6504f6:	mov    %rdi,0x80(%rsp)
  6504fe:	mov    $0x1,%edx
  650503:	shlx   %rcx,%rdx,%rdx
  650508:	mov    %rsi,%r8
  65050b:	neg    %r8
  65050e:	and    %rax,%r8
  650511:	test   %r9,%r9
  650514:	je     6507c0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3e0>
  65051a:	mov    %r9,%rax
  65051d:	shl    $0x4,%rax
  650521:	mov    %rax,0x58(%rsp)
  650526:	lea    0x0(,%r9,8),%rax
  65052e:	mov    %rax,0x68(%rsp)
  650533:	mov    $0x1,%eax
  650538:	mov    0x8(%rsp),%rcx
  65053d:	mov    %rcx,0x10(%rsp)
  650542:	xor    %edi,%edi
  650544:	mov    %rsi,0x98(%rsp)
  65054c:	mov    %rdx,0x60(%rsp)
  650551:	mov    %r9,0x38(%rsp)
  650556:	cs nopw 0x0(%rax,%rax,1)
  650560:	add    %rdx,%rdi
  650563:	cmp    0x18(%rsp),%rdi
  650568:	jae    6511a0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xdc0>
  65056e:	cmp    0x30(%rsp),%rdi
  650573:	jae    6511c0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xde0>
  650579:	sub    %rsi,%r8
  65057c:	jb     651185 <valar_spiral_rs::ntt::avx2::ntt_forward+0xda5>
  650582:	mov    %rax,0x88(%rsp)
  65058a:	mov    %r8,0x90(%rsp)
  650592:	mov    0x50(%rsp),%rax
  650597:	mov    (%rax,%rdi,8),%rax
  65059b:	mov    %rax,0xa8(%rsp)
  6505a3:	mov    0x48(%rsp),%rax
  6505a8:	mov    (%rax,%rdi,8),%rax
  6505ac:	mov    %rax,0xa0(%rsp)
  6505b4:	mov    0x68(%rsp),%rax
  6505b9:	mov    0x10(%rsp),%rcx
  6505be:	add    %rcx,%rax
  6505c1:	mov    %rax,0x70(%rsp)
  6505c6:	xor    %ebp,%ebp
  6505c8:	nopl   0x0(%rax,%rax,1)
  6505d0:	cmp    %rbp,%rsi
  6505d3:	je     65112c <valar_spiral_rs::ntt::avx2::ntt_forward+0xd4c>
  6505d9:	lea    (%r9,%rbp,1),%rdi
  6505dd:	cmp    %rsi,%rdi
  6505e0:	jae    651120 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd40>
  6505e6:	mov    0x10(%rsp),%rax
  6505eb:	mov    (%rax,%rbp,8),%r15
  6505ef:	mov    0x70(%rsp),%rax
  6505f4:	mov    (%rax,%rbp,8),%rdx
  6505f8:	mov    0x20(%rsp),%rax
  6505fd:	cmp    %rax,%r15
  650600:	mov    $0x0,%ecx
  650605:	cmovae %rax,%rcx
  650609:	mov    0xa0(%rsp),%rax
  650611:	mulx   %rax,%rax,%rax
  650616:	sub    %rcx,%r15
  650619:	mulx   0xa8(%rsp),%r12,%r13
  650623:	mov    %rax,%rdx
  650626:	mulx   %rbx,%rcx,%rax
  65062b:	sub    %rcx,%r12
  65062e:	sbb    %rax,%r13
  650631:	andn   %rbx,%r12,%rax
  650636:	mov    %rax,%rcx
  650639:	shr    $1,%rcx
  65063c:	or     %rax,%rcx
  65063f:	mov    %rcx,%rax
  650642:	shr    $0x2,%rax
  650646:	or     %rcx,%rax
  650649:	mov    %rax,%rcx
  65064c:	shr    $0x4,%rcx
  650650:	or     %rax,%rcx
  650653:	mov    %rcx,%rax
  650656:	shr    $0x8,%rax
  65065a:	or     %rcx,%rax
  65065d:	mov    %rax,%rcx
  650660:	shr    $0x10,%rcx
  650664:	or     %rax,%rcx
  650667:	mov    %rcx,%rax
  65066a:	shr    $0x20,%rax
  65066e:	or     %rbx,%rcx
  650671:	or     %rax,%rcx
  650674:	andn   %r12,%rcx,%rax
  650679:	mov    %r13,%rcx
  65067c:	shld   $0x3f,%rax,%rcx
  650681:	mov    %r13,%rdx
  650684:	shr    $1,%rdx
  650687:	or     %rax,%rcx
  65068a:	or     %r13,%rdx
  65068d:	mov    %rdx,%rax
  650690:	shr    $0x2,%rax
  650694:	or     %rdx,%rax
  650697:	shld   $0x3e,%rcx,%rdx
  65069c:	or     %rcx,%rdx
  65069f:	mov    %rax,%rcx
  6506a2:	shr    $0x4,%rcx
  6506a6:	or     %rax,%rcx
  6506a9:	shld   $0x3c,%rdx,%rax
  6506ae:	or     %rdx,%rax
  6506b1:	mov    %rcx,%rdx
  6506b4:	shr    $0x8,%rdx
  6506b8:	or     %rcx,%rdx
  6506bb:	shld   $0x38,%rax,%rcx
  6506c0:	or     %rax,%rcx
  6506c3:	mov    %rdx,%rax
  6506c6:	shr    $0x10,%rax
  6506ca:	or     %rdx,%rax
  6506cd:	shld   $0x30,%rcx,%rdx
  6506d2:	or     %rcx,%rdx
  6506d5:	mov    %rdx,%rcx
  6506d8:	shr    $0x20,%rcx
  6506dc:	mov    %rax,%rdi
  6506df:	shr    $0x20,%rdi
  6506e3:	or     %edx,%ecx
  6506e5:	or     %eax,%edi
  6506e7:	or     %ecx,%edi
  6506e9:	and    $0x1,%dil
  6506ed:	movzbl %dil,%edi
  6506f1:	call   64f3b0 <subtle::black_box>
  6506f6:	not    %al
  6506f8:	and    $0x1,%al
  6506fa:	movzbl %al,%edi
  6506fd:	call   64f3b0 <subtle::black_box>
  650702:	mov    %eax,%r14d
  650705:	mov    %r12,%rax
  650708:	xor    %rbx,%rax
  65070b:	xor    %edi,%edi
  65070d:	or     %r13,%rax
  650710:	sete   %dil
  650714:	call   64f3b0 <subtle::black_box>
  650719:	not    %al
  65071b:	and    $0x1,%al
  65071d:	movzbl %al,%edi
  650720:	call   64f3b0 <subtle::black_box>
  650725:	and    %r14b,%al
  650728:	movzbl %al,%edi
  65072b:	call   64f3b0 <subtle::black_box>
  650730:	mov    0x38(%rsp),%r9
  650735:	mov    0x98(%rsp),%rsi
  65073d:	mov    %r12,%rcx
  650740:	sub    %rbx,%rcx
  650743:	movzbl %al,%eax
  650746:	mov    %rax,%rdx
  650749:	neg    %rdx
  65074c:	dec    %rax
  65074f:	and    %rcx,%rax
  650752:	and    %r12,%rdx
  650755:	or     %rax,%rdx
  650758:	lea    (%r15,%rdx,1),%rax
  65075c:	mov    0x10(%rsp),%rcx
  650761:	mov    %rax,(%rcx,%rbp,8)
  650765:	add    0x20(%rsp),%r15
  65076a:	sub    %rdx,%r15
  65076d:	mov    0x70(%rsp),%rax
  650772:	mov    %r15,(%rax,%rbp,8)
  650776:	inc    %rbp
  650779:	cmp    %rbp,%r9
  65077c:	jne    6505d0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x1f0>
  650782:	mov    0x60(%rsp),%rdx
  650787:	mov    0x88(%rsp),%rdi
  65078f:	cmp    %rdx,%rdi
  650792:	mov    %rdi,%rax
  650795:	adc    $0x0,%rax
  650799:	mov    0x10(%rsp),%rcx
  65079e:	add    0x58(%rsp),%rcx
  6507a3:	mov    %rcx,0x10(%rsp)
  6507a8:	cmp    %rdx,%rdi
  6507ab:	mov    0x90(%rsp),%r8
  6507b3:	jb     650560 <valar_spiral_rs::ntt::avx2::ntt_forward+0x180>
  6507b9:	jmp    6504c0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe0>
  6507be:	xchg   %ax,%ax
  6507c0:	add    %rsi,%r8
  6507c3:	xor    %eax,%eax
  6507c5:	data16 cs nopw 0x0(%rax,%rax,1)
  6507d0:	lea    (%rdx,%rax,1),%rcx
  6507d4:	cmp    0x18(%rsp),%rcx
  6507d9:	jae    651191 <valar_spiral_rs::ntt::avx2::ntt_forward+0xdb1>
  6507df:	cmp    0x30(%rsp),%rcx
  6507e4:	jae    6511b1 <valar_spiral_rs::ntt::avx2::ntt_forward+0xdd1>
  6507ea:	sub    %rsi,%r8
  6507ed:	cmp    %rsi,%r8
  6507f0:	jb     651185 <valar_spiral_rs::ntt::avx2::ntt_forward+0xda5>
  6507f6:	inc    %rax
  6507f9:	cmp    %rdx,%rax
  6507fc:	jb     6507d0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x3f0>
  6507fe:	jmp    6504c0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe0>
  650803:	test   %r9,%r9
  650806:	je     65110b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd2b>
  65080c:	mov    0x38(%rdi),%r10
  650810:	mov    %r10d,%r11d
  650813:	and    $0x3f,%r11d
  650817:	mov    $0x1,%eax
  65081c:	shlx   %r11,%rax,%rsi
  650821:	mov    0x8(%rdi),%r14
  650825:	mov    0x10(%rdi),%rax
  650829:	mov    %rax,0x18(%rsp)
  65082e:	mov    %rsi,%rax
  650831:	shr    $0x2,%rax
  650835:	cmp    $0x2,%r11d
  650839:	adc    $0x0,%rax
  65083d:	mov    %rax,0x48(%rsp)
  650842:	mov    %rsi,%r15
  650845:	and    $0xfffffffffffffff0,%r15
  650849:	mov    %esi,%r12d
  65084c:	and    $0xc,%r12d
  650850:	mov    0x8(%rsp),%rax
  650855:	add    $0x60,%rax
  650859:	mov    %rax,0x78(%rsp)
  65085e:	xor    %ebp,%ebp
  650860:	vbroadcasti128 -0x5d74c9(%rip),%ymm0        # 793a0 <GCC_except_table6770+0x1150>
  650869:	vpbroadcastq -0x5d4452(%rip),%ymm1        # 7c420 <GCC_except_table6770+0x41d0>
  650872:	mov    $0x1,%eax
  650877:	xor    %ecx,%ecx
  650879:	mov    %rdi,0x68(%rsp)
  65087e:	mov    %r9,0x60(%rsp)
  650883:	mov    %r10,0x40(%rsp)
  650888:	mov    %r11,0x58(%rsp)
  65088d:	mov    %rsi,0x98(%rsp)
  650895:	mov    %r14,0x50(%rsp)
  65089a:	mov    %r15,0x80(%rsp)
  6508a2:	mov    %r12,0x28(%rsp)
  6508a7:	jmp    6508cb <valar_spiral_rs::ntt::avx2::ntt_forward+0x4eb>
  6508a9:	nopl   0x0(%rax)
  6508b0:	mov    0x88(%rsp),%rcx
  6508b8:	cmp    %r9,%rcx
  6508bb:	mov    %rcx,%rax
  6508be:	adc    $0x0,%rax
  6508c2:	cmp    %r9,%rcx
  6508c5:	jae    65110b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd2b>
  6508cb:	cmp    0x18(%rsp),%rcx
  6508d0:	jae    65120f <valar_spiral_rs::ntt::avx2::ntt_forward+0xe2f>
  6508d6:	mov    %rax,%r8
  6508d9:	lea    (%rcx,%rcx,2),%rdx
  6508dd:	mov    0x10(%r14,%rdx,8),%rax
  6508e2:	test   %rax,%rax
  6508e5:	je     6511e3 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe03>
  6508eb:	cmp    $0x1,%rax
  6508ef:	je     6511f6 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe16>
  6508f5:	mov    %r8,0x88(%rsp)
  6508fd:	cmp    $0x3,%rcx
  650901:	ja     651226 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe46>
  650907:	shlx   %r11,%rcx,%rbx
  65090c:	mov    0x8(%rsp),%rax
  650911:	lea    (%rax,%rbx,8),%r8
  650915:	mov    0xa8(%rdi,%rcx,8),%rax
  65091d:	lea    (%rax,%rax,1),%ecx
  650920:	mov    %eax,%eax
  650922:	test   %r10,%r10
  650925:	je     650cdb <valar_spiral_rs::ntt::avx2::ntt_forward+0x8fb>
  65092b:	mov    %r8,0x38(%rsp)
  650930:	lea    (%r14,%rdx,8),%rdx
  650934:	mov    0x8(%rdx),%rdx
  650938:	mov    0x8(%rdx),%r13
  65093c:	mov    0x20(%rdx),%rdi
  650940:	vmovq  %rcx,%xmm2
  650945:	vpbroadcastq %xmm2,%ymm2
  65094a:	vmovq  %rax,%xmm3
  65094f:	vpbroadcastq %xmm3,%ymm3
  650954:	vmovd  %ecx,%xmm4
  650958:	vpbroadcastd %xmm4,%xmm4
  65095d:	mov    0x8(%rsp),%rdx
  650962:	lea    (%rdx,%rbx,8),%rdx
  650966:	mov    %rdx,0x90(%rsp)
  65096e:	shl    $0x3,%rbx
  650972:	mov    %rbx,0x30(%rsp)
  650977:	mov    $0x1,%edx
  65097c:	xor    %r8d,%r8d
  65097f:	mov    %r13,0x20(%rsp)
  650984:	mov    %rdi,0x10(%rsp)
  650989:	jmp    6509b2 <valar_spiral_rs::ntt::avx2::ntt_forward+0x5d2>
  65098b:	nopl   0x0(%rax,%rax,1)
  650990:	mov    0xa0(%rsp),%r8
  650998:	lea    0x1(%r8),%rdx
  65099c:	mov    0x40(%rsp),%r10
  6509a1:	cmp    %r10,%r8
  6509a4:	mov    0x98(%rsp),%rsi
  6509ac:	je     650ca0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x8c0>
  6509b2:	mov    %r8,%rdi
  6509b5:	mov    $0x1,%r8d
  6509bb:	shlx   %rdi,%r8,%r15
  6509c0:	mov    %rdx,%rdi
  6509c3:	cmp    $0xb,%r10
  6509c7:	setb   %dl
  6509ca:	mov    %rdi,0xa0(%rsp)
  6509d2:	shrx   %rdi,%rsi,%r12
  6509d7:	cmp    $0x4,%r12
  6509db:	setb   %sil
  6509df:	or     %dl,%sil
  6509e2:	mov    %r12,%rdx
  6509e5:	shr    $0x2,%rdx
  6509e9:	mov    %r12d,%r8d
  6509ec:	and    $0x3,%r8d
  6509f0:	cmp    $0x1,%r8
  6509f4:	sbb    $0xffffffffffffffff,%rdx
  6509f8:	test   %sil,%sil
  6509fb:	je     650bb0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7d0>
  650a01:	test   %r12,%r12
  650a04:	mov    0x10(%rsp),%rdi
  650a09:	je     650990 <valar_spiral_rs::ntt::avx2::ntt_forward+0x5b0>
  650a0b:	mov    %r12,%r11
  650a0e:	and    $0xfffffffffffffffc,%r11
  650a12:	mov    %r12,%rdx
  650a15:	shl    $0x4,%rdx
  650a19:	mov    %rdx,0x70(%rsp)
  650a1e:	lea    0x0(,%r12,8),%rdx
  650a26:	mov    %rdx,0xa8(%rsp)
  650a2e:	mov    0x90(%rsp),%rdx
  650a36:	lea    (%rdx,%r12,8),%rbx
  650a3a:	mov    $0x1,%r9d
  650a40:	mov    0x38(%rsp),%r14
  650a45:	xor    %edx,%edx
  650a47:	jmp    650a78 <valar_spiral_rs::ntt::avx2::ntt_forward+0x698>
  650a49:	nopl   0x0(%rax)
  650a50:	cmp    %r15,%rdx
  650a53:	mov    %rdx,%r9
  650a56:	adc    $0x0,%r9
  650a5a:	mov    0x70(%rsp),%rsi
  650a5f:	add    %rsi,%r14
  650a62:	add    %rsi,%rbx
  650a65:	cmp    %r15,%rdx
  650a68:	mov    0x20(%rsp),%r13
  650a6d:	mov    0x10(%rsp),%rdi
  650a72:	jae    650990 <valar_spiral_rs::ntt::avx2::ntt_forward+0x5b0>
  650a78:	add    %r15,%rdx
  650a7b:	mov    0x0(%r13,%rdx,8),%rsi
  650a80:	mov    (%rdi,%rdx,8),%r8
  650a84:	mov    %r9,%rdx
  650a87:	cmp    $0x4,%r12
  650a8b:	jae    650aa0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6c0>
  650a8d:	xor    %r13d,%r13d
  650a90:	jmp    650b60 <valar_spiral_rs::ntt::avx2::ntt_forward+0x780>
  650a95:	data16 cs nopw 0x0(%rax,%rax,1)
  650aa0:	vmovq  %rsi,%xmm5
  650aa5:	vpbroadcastq %xmm5,%ymm5
  650aaa:	vmovq  %r8,%xmm6
  650aaf:	vpbroadcastq %xmm6,%ymm6
  650ab4:	mov    0xa8(%rsp),%rdi
  650abc:	lea    (%r14,%rdi,1),%r13
  650ac0:	vpsrlq $0x20,%ymm6,%ymm7
  650ac5:	vpsrlq $0x20,%ymm5,%ymm8
  650aca:	xor    %r9d,%r9d
  650acd:	nopl   (%rax)
  650ad0:	vpermd (%r14,%r9,8),%ymm0,%ymm9
  650ad6:	vmovdqu 0x0(%r13,%r9,8),%ymm10
  650add:	vpminud %xmm9,%xmm4,%xmm11
  650ae2:	vpcmpeqd %xmm4,%xmm11,%xmm11
  650ae6:	vpand  %xmm4,%xmm11,%xmm11
  650aea:	vpsubd %xmm11,%xmm9,%xmm9
  650aef:	vpmuludq %ymm6,%ymm10,%ymm11
  650af3:	vpmuludq %ymm7,%ymm10,%ymm12
  650af7:	vpsllq $0x20,%ymm12,%ymm12
  650afd:	vpaddq %ymm12,%ymm11,%ymm11
  650b02:	vpsrlq $0x20,%ymm11,%ymm11
  650b08:	vpmuludq %ymm5,%ymm10,%ymm12
  650b0c:	vpmuludq %ymm8,%ymm10,%ymm10
  650b11:	vpsllq $0x20,%ymm10,%ymm10
  650b17:	vpaddq %ymm10,%ymm12,%ymm10
  650b1c:	vpmuludq %ymm3,%ymm11,%ymm11
  650b20:	vpsubq %ymm11,%ymm10,%ymm10
  650b25:	vpmovzxdq %xmm9,%ymm9
  650b2a:	vpaddq %ymm9,%ymm10,%ymm11
  650b2f:	vmovdqu %ymm11,(%r14,%r9,8)
  650b35:	vpaddq %ymm2,%ymm9,%ymm9
  650b39:	vpsubq %ymm10,%ymm9,%ymm9
  650b3e:	vmovdqu %ymm9,0x0(%r13,%r9,8)
  650b45:	add    $0x4,%r9
  650b49:	cmp    %r9,%r11
  650b4c:	jne    650ad0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x6f0>
  650b4e:	mov    %r11,%r13
  650b51:	cmp    %r11,%r12
  650b54:	je     650a50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x670>
  650b5a:	nopw   0x0(%rax,%rax,1)
  650b60:	mov    (%r14,%r13,8),%r9d
  650b64:	mov    (%rbx,%r13,8),%edi
  650b68:	cmp    %r9d,%ecx
  650b6b:	mov    %ecx,%r10d
  650b6e:	cmova  %ebp,%r10d
  650b72:	sub    %r10d,%r9d
  650b75:	mov    %rdi,%r10
  650b78:	imul   %r8,%r10
  650b7c:	shr    $0x20,%r10
  650b80:	imul   %rsi,%rdi
  650b84:	imul   %rax,%r10
  650b88:	sub    %r10,%rdi
  650b8b:	lea    (%rdi,%r9,1),%r10
  650b8f:	mov    %r10,(%r14,%r13,8)
  650b93:	add    %rcx,%r9
  650b96:	sub    %rdi,%r9
  650b99:	mov    %r9,(%rbx,%r13,8)
  650b9d:	inc    %r13
  650ba0:	cmp    %r13,%r12
  650ba3:	jne    650b60 <valar_spiral_rs::ntt::avx2::ntt_forward+0x780>
  650ba5:	jmp    650a50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x670>
  650baa:	nopw   0x0(%rax,%rax,1)
  650bb0:	mov    %r12,%rsi
  650bb3:	shl    $0x4,%rsi
  650bb7:	mov    $0x1,%r9d
  650bbd:	mov    0x38(%rsp),%r8
  650bc2:	xor    %r10d,%r10d
  650bc5:	mov    0x10(%rsp),%rdi
  650bca:	nopw   0x0(%rax,%rax,1)
  650bd0:	add    %r15,%r10
  650bd3:	vpmovzxdq (%rdi,%r10,8),%xmm5
  650bd9:	vpmovzxdq 0x0(%r13,%r10,8),%xmm6
  650be0:	mov    %r9,%r10
  650be3:	vpbroadcastq %xmm5,%ymm5
  650be8:	vpbroadcastq %xmm6,%ymm6
  650bed:	mov    %r8,%r11
  650bf0:	mov    %rdx,%rbx
  650bf3:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  650c00:	vmovdqa (%r11),%ymm7
  650c05:	vmovdqa (%r11,%r12,8),%ymm8
  650c0b:	vpcmpgtq %ymm2,%ymm7,%ymm9
  650c10:	vpand  %ymm2,%ymm9,%ymm9
  650c14:	vpsubq %ymm9,%ymm7,%ymm7
  650c19:	vpmuludq %ymm5,%ymm8,%ymm9
  650c1d:	vpsrlq $0x20,%ymm5,%ymm10
  650c22:	vpmuludq %ymm10,%ymm8,%ymm10
  650c27:	vpsllq $0x20,%ymm10,%ymm10
  650c2d:	vpaddq %ymm10,%ymm9,%ymm9
  650c32:	vpsrlq $0x20,%ymm9,%ymm9
  650c38:	vpmuludq %ymm6,%ymm8,%ymm10
  650c3c:	vpsrlq $0x20,%ymm6,%ymm11
  650c41:	vpmuludq %ymm11,%ymm8,%ymm8
  650c46:	vpsllq $0x20,%ymm8,%ymm8
  650c4c:	vpaddq %ymm8,%ymm10,%ymm8
  650c51:	vpmuludq %ymm3,%ymm9,%ymm9
  650c55:	vpsubq %ymm9,%ymm8,%ymm8
  650c5a:	vpaddq %ymm7,%ymm8,%ymm9
  650c5e:	vpaddq %ymm2,%ymm7,%ymm7
  650c62:	vpsubq %ymm8,%ymm7,%ymm7
  650c67:	vmovdqa %ymm9,(%r11)
  650c6c:	vmovdqa %ymm7,(%r11,%r12,8)
  650c72:	add    $0x20,%r11
  650c76:	dec    %rbx
  650c79:	jne    650c00 <valar_spiral_rs::ntt::avx2::ntt_forward+0x820>
  650c7b:	cmp    %r15,%r10
  650c7e:	mov    %r10,%r9
  650c81:	adc    $0x0,%r9
  650c85:	add    %rsi,%r8
  650c88:	cmp    %r15,%r10
  650c8b:	jb     650bd0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x7f0>
  650c91:	jmp    650990 <valar_spiral_rs::ntt::avx2::ntt_forward+0x5b0>
  650c96:	cs nopw 0x0(%rax,%rax,1)
  650ca0:	cmp    $0xa,%r10
  650ca4:	ja     650d20 <valar_spiral_rs::ntt::avx2::ntt_forward+0x940>
  650ca6:	mov    0x58(%rsp),%r11
  650cab:	cmp    $0x1,%r11d
  650caf:	mov    0x68(%rsp),%rdi
  650cb4:	mov    0x60(%rsp),%r9
  650cb9:	mov    0x50(%rsp),%r14
  650cbe:	mov    0x80(%rsp),%r15
  650cc6:	mov    0x28(%rsp),%r12
  650ccb:	mov    0x38(%rsp),%r8
  650cd0:	mov    0x30(%rsp),%rdx
  650cd5:	ja     650d8b <valar_spiral_rs::ntt::avx2::ntt_forward+0x9ab>
  650cdb:	xor    %edx,%edx
  650cdd:	jmp    650cf7 <valar_spiral_rs::ntt::avx2::ntt_forward+0x917>
  650cdf:	nop
  650ce0:	sub    %r8,%rbx
  650ce3:	mov    %r13,%r8
  650ce6:	mov    %rbx,0x0(%r13,%rdx,8)
  650ceb:	inc    %rdx
  650cee:	cmp    %rdx,%rsi
  650cf1:	je     6508b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4d0>
  650cf7:	mov    %r8,%r13
  650cfa:	mov    (%r8,%rdx,8),%rbx
  650cfe:	mov    $0x0,%r8d
  650d04:	cmp    %rcx,%rbx
  650d07:	jb     650d0c <valar_spiral_rs::ntt::avx2::ntt_forward+0x92c>
  650d09:	mov    %rcx,%r8
  650d0c:	sub    %r8,%rbx
  650d0f:	mov    $0x0,%r8d
  650d15:	cmp    %rax,%rbx
  650d18:	jb     650ce0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x900>
  650d1a:	mov    %rax,%r8
  650d1d:	jmp    650ce0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x900>
  650d1f:	nop
  650d20:	cmpq   $0x0,0x48(%rsp)
  650d26:	mov    0x68(%rsp),%rdi
  650d2b:	mov    0x60(%rsp),%r9
  650d30:	mov    0x58(%rsp),%r11
  650d35:	mov    0x50(%rsp),%r14
  650d3a:	mov    0x38(%rsp),%rdx
  650d3f:	je     6508b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4d0>
  650d45:	xor    %ecx,%ecx
  650d47:	mov    0x48(%rsp),%rax
  650d4c:	nopl   0x0(%rax)
  650d50:	cmp    %rsi,%rcx
  650d53:	jae    6511d1 <valar_spiral_rs::ntt::avx2::ntt_forward+0xdf1>
  650d59:	vmovdqa (%rdx,%rcx,8),%ymm4
  650d5e:	vpcmpgtq %ymm2,%ymm4,%ymm5
  650d63:	vpand  %ymm2,%ymm5,%ymm5
  650d67:	vpsubq %ymm5,%ymm4,%ymm4
  650d6b:	vpcmpgtq %ymm3,%ymm4,%ymm5
  650d70:	vpand  %ymm3,%ymm5,%ymm5
  650d74:	vpsubq %ymm5,%ymm4,%ymm4
  650d78:	vmovdqa %ymm4,(%rdx,%rcx,8)
  650d7d:	add    $0x4,%rcx
  650d81:	dec    %rax
  650d84:	jne    650d50 <valar_spiral_rs::ntt::avx2::ntt_forward+0x970>
  650d86:	jmp    6508b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4d0>
  650d8b:	vpor   %ymm1,%ymm2,%ymm4
  650d8f:	vpor   %ymm1,%ymm3,%ymm5
  650d93:	cmp    $0x4,%r11d
  650d97:	jae    650ddc <valar_spiral_rs::ntt::avx2::ntt_forward+0x9fc>
  650d99:	xor    %eax,%eax
  650d9b:	nopl   0x0(%rax,%rax,1)
  650da0:	vmovdqu (%r8,%rax,8),%ymm6
  650da6:	vpxor  %ymm1,%ymm6,%ymm7
  650daa:	vpcmpgtq %ymm7,%ymm4,%ymm7
  650daf:	vpandn %ymm2,%ymm7,%ymm7
  650db3:	vpsubq %ymm7,%ymm6,%ymm6
  650db7:	vpxor  %ymm1,%ymm6,%ymm7
  650dbb:	vpcmpgtq %ymm7,%ymm5,%ymm7
  650dc0:	vpandn %ymm3,%ymm7,%ymm7
  650dc4:	vpsubq %ymm7,%ymm6,%ymm6
  650dc8:	vmovdqu %ymm6,(%r8,%rax,8)
  650dce:	add    $0x4,%rax
  650dd2:	cmp    %rax,%r12
  650dd5:	jne    650da0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x9c0>
  650dd7:	jmp    6508b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4d0>
  650ddc:	add    0x78(%rsp),%rdx
  650de1:	xor    %eax,%eax
  650de3:	data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  650df0:	vmovdqu -0x60(%rdx,%rax,8),%ymm6
  650df6:	vmovdqu -0x40(%rdx,%rax,8),%ymm7
  650dfc:	vmovdqu -0x20(%rdx,%rax,8),%ymm8
  650e02:	vmovdqu (%rdx,%rax,8),%ymm9
  650e07:	vpxor  %ymm1,%ymm6,%ymm10
  650e0b:	vpcmpgtq %ymm10,%ymm4,%ymm10
  650e10:	vpandn %ymm2,%ymm10,%ymm10
  650e14:	vpxor  %ymm1,%ymm7,%ymm11
  650e18:	vpcmpgtq %ymm11,%ymm4,%ymm11
  650e1d:	vpandn %ymm2,%ymm11,%ymm11
  650e21:	vpxor  %ymm1,%ymm8,%ymm12
  650e25:	vpcmpgtq %ymm12,%ymm4,%ymm12
  650e2a:	vpandn %ymm2,%ymm12,%ymm12
  650e2e:	vpxor  %ymm1,%ymm9,%ymm13
  650e32:	vpcmpgtq %ymm13,%ymm4,%ymm13
  650e37:	vpandn %ymm2,%ymm13,%ymm13
  650e3b:	vpsubq %ymm10,%ymm6,%ymm6
  650e40:	vpsubq %ymm11,%ymm7,%ymm7
  650e45:	vpsubq %ymm12,%ymm8,%ymm8
  650e4a:	vpsubq %ymm13,%ymm9,%ymm9
  650e4f:	vpxor  %ymm1,%ymm6,%ymm10
  650e53:	vpcmpgtq %ymm10,%ymm5,%ymm10
  650e58:	vpandn %ymm3,%ymm10,%ymm10
  650e5c:	vpxor  %ymm1,%ymm7,%ymm11
  650e60:	vpcmpgtq %ymm11,%ymm5,%ymm11
  650e65:	vpandn %ymm3,%ymm11,%ymm11
  650e69:	vpxor  %ymm1,%ymm8,%ymm12
  650e6d:	vpcmpgtq %ymm12,%ymm5,%ymm12
  650e72:	vpandn %ymm3,%ymm12,%ymm12
  650e76:	vpxor  %ymm1,%ymm9,%ymm13
  650e7a:	vpcmpgtq %ymm13,%ymm5,%ymm13
  650e7f:	vpandn %ymm3,%ymm13,%ymm13
  650e83:	vpsubq %ymm10,%ymm6,%ymm6
  650e88:	vpsubq %ymm11,%ymm7,%ymm7
  650e8d:	vpsubq %ymm12,%ymm8,%ymm8
  650e92:	vpsubq %ymm13,%ymm9,%ymm9
  650e97:	vmovdqu %ymm6,-0x60(%rdx,%rax,8)
  650e9d:	vmovdqu %ymm7,-0x40(%rdx,%rax,8)
  650ea3:	vmovdqu %ymm8,-0x20(%rdx,%rax,8)
  650ea9:	vmovdqu %ymm9,(%rdx,%rax,8)
  650eae:	add    $0x10,%rax
  650eb2:	cmp    %rax,%r15
  650eb5:	jne    650df0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xa10>
  650ebb:	jmp    6508b0 <valar_spiral_rs::ntt::avx2::ntt_forward+0x4d0>
  650ec0:	mov    0xb0(%rsp),%rax
  650ec8:	cmp    $0x1,%eax
  650ecb:	mov    0x28(%rsp),%rsi
  650ed0:	ja     650fa2 <valar_spiral_rs::ntt::avx2::ntt_forward+0xbc2>
  650ed6:	xor    %eax,%eax
  650ed8:	jmp    650ef8 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb18>
  650eda:	nopw   0x0(%rax,%rax,1)
  650ee0:	sub    %rdx,%rcx
  650ee3:	mov    0x8(%rsp),%rdx
  650ee8:	mov    %rcx,(%rdx,%rax,8)
  650eec:	inc    %rax
  650eef:	cmp    %rax,%rsi
  650ef2:	je     65110b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd2b>
  650ef8:	mov    0x8(%rsp),%rcx
  650efd:	mov    (%rcx,%rax,8),%rcx
  650f01:	mov    $0x0,%edx
  650f06:	cmp    0x20(%rsp),%rcx
  650f0b:	jb     650f12 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb32>
  650f0d:	mov    0x20(%rsp),%rdx
  650f12:	sub    %rdx,%rcx
  650f15:	mov    $0x0,%edx
  650f1a:	cmp    0x40(%rsp),%rcx
  650f1f:	jb     650ee0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb00>
  650f21:	mov    0x40(%rsp),%rdx
  650f26:	jmp    650ee0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb00>
  650f28:	cmp    %rdx,%r9
  650f2b:	ja     651172 <valar_spiral_rs::ntt::avx2::ntt_forward+0xd92>
  650f31:	test   %rsi,%rsi
  650f34:	je     65123d <valar_spiral_rs::ntt::avx2::ntt_forward+0xe5d>
  650f3a:	mov    0x10(%rcx),%rax
  650f3e:	test   %rax,%rax
  650f41:	je     6511e3 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe03>
  650f47:	cmp    $0x1,%rax
  650f4b:	je     6511f6 <valar_spiral_rs::ntt::avx2::ntt_forward+0xe16>
  650f51:	mov    0xa8(%rdi),%rax
  650f58:	lea    (%rax,%rax,1),%rcx
  650f5c:	xor    %edx,%edx
  650f5e:	jmp    650f7a <valar_spiral_rs::ntt::avx2::ntt_forward+0xb9a>
  650f60:	sub    %rdi,%rsi
  650f63:	mov    0x8(%rsp),%rdi
  650f68:	mov    %rsi,(%rdi,%rdx,8)
  650f6c:	inc    %rdx
  650f6f:	cmp    %rdx,0x28(%rsp)
  650f74:	je     65110b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd2b>
  650f7a:	mov    0x8(%rsp),%rsi
  650f7f:	mov    (%rsi,%rdx,8),%rsi
  650f83:	mov    $0x0,%edi
  650f88:	cmp    %rcx,%rsi
  650f8b:	jb     650f90 <valar_spiral_rs::ntt::avx2::ntt_forward+0xbb0>
  650f8d:	mov    %rcx,%rdi
  650f90:	sub    %rdi,%rsi
  650f93:	mov    $0x0,%edi
  650f98:	cmp    %rax,%rsi
  650f9b:	jb     650f60 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb80>
  650f9d:	mov    %rax,%rdi
  650fa0:	jmp    650f60 <valar_spiral_rs::ntt::avx2::ntt_forward+0xb80>
  650fa2:	vmovq  0x20(%rsp),%xmm0
  650fa8:	vpbroadcastq %xmm0,%ymm0
  650fad:	vmovq  0x40(%rsp),%xmm1
  650fb3:	vpbroadcastq %xmm1,%ymm1
  650fb8:	cmp    $0x4,%eax
  650fbb:	jae    65101a <valar_spiral_rs::ntt::avx2::ntt_forward+0xc3a>
  650fbd:	and    $0xc,%esi
  650fc0:	xor    %eax,%eax
  650fc2:	vpbroadcastq -0x5d4bab(%rip),%ymm2        # 7c420 <GCC_except_table6770+0x41d0>
  650fcb:	vpxor  %ymm2,%ymm0,%ymm3
  650fcf:	vpxor  %ymm2,%ymm1,%ymm4
  650fd3:	mov    0x8(%rsp),%rcx
  650fd8:	nopl   0x0(%rax,%rax,1)
  650fe0:	vmovdqu (%rcx,%rax,8),%ymm5
  650fe5:	vpxor  %ymm2,%ymm5,%ymm6
  650fe9:	vpcmpgtq %ymm6,%ymm3,%ymm6
  650fee:	vpandn %ymm0,%ymm6,%ymm6
  650ff2:	vpsubq %ymm6,%ymm5,%ymm5
  650ff6:	vpxor  %ymm2,%ymm5,%ymm6
  650ffa:	vpcmpgtq %ymm6,%ymm4,%ymm6
  650fff:	vpandn %ymm1,%ymm6,%ymm6
  651003:	vpsubq %ymm6,%ymm5,%ymm5
  651007:	vmovdqu %ymm5,(%rcx,%rax,8)
  65100c:	add    $0x4,%rax
  651010:	cmp    %rax,%rsi
  651013:	jne    650fe0 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc00>
  651015:	jmp    65110b <valar_spiral_rs::ntt::avx2::ntt_forward+0xd2b>
  65101a:	and    $0xfffffffffffffff0,%rsi
  65101e:	xor    %eax,%eax
  651020:	vpbroadcastq -0x5d4c09(%rip),%ymm2        # 7c420 <GCC_except_table6770+0x41d0>
  651029:	vpxor  %ymm2,%ymm0,%ymm3
  65102d:	vpxor  %ymm2,%ymm1,%ymm4
  651031:	mov    0x8(%rsp),%rcx
  651036:	cs nopw 0x0(%rax,%rax,1)
  651040:	vmovdqu (%rcx,%rax,8),%ymm5
  651045:	vmovdqu 0x20(%rcx,%rax,8),%ymm6
  65104b:	vmovdqu 0x40(%rcx,%rax,8),%ymm7
  651051:	vmovdqu 0x60(%rcx,%rax,8),%ymm8
  651057:	vpxor  %ymm2,%ymm5,%ymm9
  65105b:	vpcmpgtq %ymm9,%ymm3,%ymm9
  651060:	vpandn %ymm0,%ymm9,%ymm9
  651064:	vpxor  %ymm2,%ymm6,%ymm10
  651068:	vpcmpgtq %ymm10,%ymm3,%ymm10
  65106d:	vpandn %ymm0,%ymm10,%ymm10
  651071:	vpxor  %ymm2,%ymm7,%ymm11
  651075:	vpcmpgtq %ymm11,%ymm3,%ymm11
  65107a:	vpandn %ymm0,%ymm11,%ymm11
  65107e:	vpxor  %ymm2,%ymm8,%ymm12
  651082:	vpcmpgtq %ymm12,%ymm3,%ymm12
  651087:	vpandn %ymm0,%ymm12,%ymm12
  65108b:	vpsubq %ymm9,%ymm5,%ymm5
  651090:	vpsubq %ymm10,%ymm6,%ymm6
  651095:	vpsubq %ymm11,%ymm7,%ymm7
  65109a:	vpsubq %ymm12,%ymm8,%ymm8
  65109f:	vpxor  %ymm2,%ymm5,%ymm9
  6510a3:	vpcmpgtq %ymm9,%ymm4,%ymm9
  6510a8:	vpandn %ymm1,%ymm9,%ymm9
  6510ac:	vpxor  %ymm2,%ymm6,%ymm10
  6510b0:	vpcmpgtq %ymm10,%ymm4,%ymm10
  6510b5:	vpandn %ymm1,%ymm10,%ymm10
  6510b9:	vpxor  %ymm2,%ymm7,%ymm11
  6510bd:	vpcmpgtq %ymm11,%ymm4,%ymm11
  6510c2:	vpandn %ymm1,%ymm11,%ymm11
  6510c6:	vpxor  %ymm2,%ymm8,%ymm12
  6510ca:	vpcmpgtq %ymm12,%ymm4,%ymm12
  6510cf:	vpandn %ymm1,%ymm12,%ymm12
  6510d3:	vpsubq %ymm9,%ymm5,%ymm5
  6510d8:	vpsubq %ymm10,%ymm6,%ymm6
  6510dd:	vpsubq %ymm11,%ymm7,%ymm7
  6510e2:	vpsubq %ymm12,%ymm8,%ymm8
  6510e7:	vmovdqu %ymm5,(%rcx,%rax,8)
  6510ec:	vmovdqu %ymm6,0x20(%rcx,%rax,8)
  6510f2:	vmovdqu %ymm7,0x40(%rcx,%rax,8)
  6510f8:	vmovdqu %ymm8,0x60(%rcx,%rax,8)
  6510fe:	add    $0x10,%rax
  651102:	cmp    %rax,%rsi
  651105:	jne    651040 <valar_spiral_rs::ntt::avx2::ntt_forward+0xc60>
  65110b:	add    $0xe8,%rsp
  651112:	pop    %rbx
  651113:	pop    %r12
  651115:	pop    %r13
  651117:	pop    %r14
  651119:	pop    %r15
  65111b:	pop    %rbp
  65111c:	vzeroupper
  65111f:	ret
  651120:	lea    0x848a9(%rip),%rdx        # 6d59d0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2438>
  651127:	call   28beb0 <core::panicking::panic_bounds_check>
  65112c:	lea    0x84885(%rip),%rdx        # 6d59b8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2420>
  651133:	mov    %rbp,%rdi
  651136:	call   28beb0 <core::panicking::panic_bounds_check>
  65113b:	lea    0x84de6(%rip),%rax        # 6d5f28 <tokio::runtime::task::waker::WAKER_VTABLE+0x2990>
  651142:	mov    %rax,0xb8(%rsp)
  65114a:	vmovaps -0x5d5812(%rip),%ymm0        # 7b940 <GCC_except_table6770+0x36f0>
  651152:	vmovups %ymm0,0xc0(%rsp)
  65115b:	lea    0x847f6(%rip),%rsi        # 6d5958 <tokio::runtime::task::waker::WAKER_VTABLE+0x23c0>
  651162:	lea    0xb8(%rsp),%rdi
  65116a:	vzeroupper
  65116d:	call   288420 <core::panicking::panic_fmt>
  651172:	lea    0x8486f(%rip),%rcx        # 6d59e8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2450>
  651179:	xor    %edi,%edi
  65117b:	mov    0x28(%rsp),%rsi
  651180:	call   289700 <core::slice::index::slice_index_fail>
  651185:	lea    0x84814(%rip),%rdi        # 6d59a0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2408>
  65118c:	call   288440 <core::option::unwrap_failed>
  651191:	mov    0x18(%rsp),%rax
  651196:	cmp    %rdx,%rax
  651199:	cmova  %rax,%rdx
  65119d:	mov    %rdx,%rdi
  6511a0:	lea    0x847c9(%rip),%rdx        # 6d5970 <tokio::runtime::task::waker::WAKER_VTABLE+0x23d8>
  6511a7:	mov    0x18(%rsp),%rsi
  6511ac:	call   28beb0 <core::panicking::panic_bounds_check>
  6511b1:	mov    0x30(%rsp),%rax
  6511b6:	cmp    %rdx,%rax
  6511b9:	cmova  %rax,%rdx
  6511bd:	mov    %rdx,%rdi
  6511c0:	lea    0x847c1(%rip),%rdx        # 6d5988 <tokio::runtime::task::waker::WAKER_VTABLE+0x23f0>
  6511c7:	mov    0x30(%rsp),%rsi
  6511cc:	call   28beb0 <core::panicking::panic_bounds_check>
  6511d1:	lea    0x848e8(%rip),%rdx        # 6d5ac0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2528>
  6511d8:	mov    %rcx,%rdi
  6511db:	vzeroupper
  6511de:	call   28beb0 <core::panicking::panic_bounds_check>
  6511e3:	lea    0x84dde(%rip),%rdx        # 6d5fc8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a30>
  6511ea:	xor    %edi,%edi
  6511ec:	xor    %esi,%esi
  6511ee:	vzeroupper
  6511f1:	call   28beb0 <core::panicking::panic_bounds_check>
  6511f6:	lea    0x84e13(%rip),%rdx        # 6d6010 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a78>
  6511fd:	mov    $0x1,%edi
  651202:	mov    $0x1,%esi
  651207:	vzeroupper
  65120a:	call   28beb0 <core::panicking::panic_bounds_check>
  65120f:	lea    0x84d9a(%rip),%rdx        # 6d5fb0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a18>
  651216:	mov    %rcx,%rdi
  651219:	mov    0x18(%rsp),%rsi
  65121e:	vzeroupper
  651221:	call   28beb0 <core::panicking::panic_bounds_check>
  651226:	lea    0x8487b(%rip),%rdx        # 6d5aa8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2510>
  65122d:	mov    $0x4,%esi
  651232:	mov    %rcx,%rdi
  651235:	vzeroupper
  651238:	call   28beb0 <core::panicking::panic_bounds_check>
  65123d:	lea    0x84d6c(%rip),%rdx        # 6d5fb0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2a18>
  651244:	xor    %edi,%edi
  651246:	call   28beb0 <core::panicking::panic_bounds_check>
  65124b:	int3
  65124c:	int3
  65124d:	int3
  65124e:	int3
  65124f:	int3

0000000000651250 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>>:
  651250:	push   %rbp
  651251:	push   %r15
  651253:	push   %r14
  651255:	push   %r13
  651257:	push   %r12
  651259:	push   %rbx
  65125a:	sub    $0x18,%rsp
  65125e:	mov    0x8(%rdi),%rbx
  651262:	mov    %rdi,0x8(%rsp)
  651267:	mov    0x10(%rdi),%rax
  65126b:	mov    %rax,0x10(%rsp)
  651270:	test   %rax,%rax
  651273:	je     651305 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0xb5>
  651279:	xor    %r13d,%r13d
  65127c:	mov    0x865d5(%rip),%rbp        # 6d7858 <free@GLIBC_2.2.5>
  651283:	jmp    65129a <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0x4a>
  651285:	data16 cs nopw 0x0(%rax,%rax,1)
  651290:	inc    %r13
  651293:	cmp    0x10(%rsp),%r13
  651298:	je     651305 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0xb5>
  65129a:	lea    0x0(,%r13,2),%r14
  6512a2:	add    %r13,%r14
  6512a5:	mov    0x8(%rbx,%r14,8),%rax
  6512aa:	mov    %rax,(%rsp)
  6512ae:	mov    %rbx,%r15
  6512b1:	mov    0x10(%rbx,%r14,8),%r12
  6512b6:	test   %r12,%r12
  6512b9:	je     6512f0 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0xa0>
  6512bb:	mov    (%rsp),%rax
  6512bf:	lea    0x8(%rax),%rbx
  6512c3:	jmp    6512d9 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0x89>
  6512c5:	data16 cs nopw 0x0(%rax,%rax,1)
  6512d0:	add    $0x18,%rbx
  6512d4:	dec    %r12
  6512d7:	je     6512f0 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0xa0>
  6512d9:	cmpq   $0x0,-0x8(%rbx)
  6512de:	je     6512d0 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0x80>
  6512e0:	mov    (%rbx),%rdi
  6512e3:	call   *%rbp
  6512e5:	jmp    6512d0 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0x80>
  6512e7:	nopw   0x0(%rax,%rax,1)
  6512f0:	mov    %r15,%rbx
  6512f3:	lea    (%r15,%r14,8),%rax
  6512f7:	cmpq   $0x0,(%rax)
  6512fb:	je     651290 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0x40>
  6512fd:	mov    (%rsp),%rdi
  651301:	call   *%rbp
  651303:	jmp    651290 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0x40>
  651305:	mov    0x8(%rsp),%rax
  65130a:	cmpq   $0x0,(%rax)
  65130e:	je     651327 <core::ptr::drop_in_place<alloc::vec::Vec<alloc::vec::Vec<alloc::vec::Vec<u64>>>>+0xd7>
  651310:	mov    %rbx,%rdi
  651313:	add    $0x18,%rsp
  651317:	pop    %rbx
  651318:	pop    %r12
  65131a:	pop    %r13
  65131c:	pop    %r14
  65131e:	pop    %r15
  651320:	pop    %rbp
  651321:	jmp    *0x86531(%rip)        # 6d7858 <free@GLIBC_2.2.5>
  651327:	add    $0x18,%rsp
  65132b:	pop    %rbx
  65132c:	pop    %r12
  65132e:	pop    %r13
  651330:	pop    %r14
  651332:	pop    %r15
  651334:	pop    %rbp
  651335:	ret
  651336:	int3
  651337:	int3
  651338:	int3
  651339:	int3
  65133a:	int3
  65133b:	int3
  65133c:	int3
  65133d:	int3
  65133e:	int3
  65133f:	int3

0000000000651340 <valar_spiral_rs::poly::to_ntt_alloc>:
  651340:	push   %rbp
  651341:	push   %r15
  651343:	push   %r14
  651345:	push   %r13
  651347:	push   %r12
  651349:	push   %rbx
  65134a:	sub    $0x48,%rsp
  65134e:	mov    0x20(%rsi),%rbp
  651352:	mov    0x28(%rsi),%rax
  651356:	mov    0x30(%rsi),%r14
  65135a:	mov    %r14,%r13
  65135d:	mov    %rax,0x8(%rsp)
  651362:	imul   %rax,%r13
  651366:	imul   0x30(%rbp),%r13
  65136b:	imul   0x40(%rbp),%r13
  651370:	mov    %rdi,(%rsp)
  651374:	lea    0x0(,%r13,8),%r15
  65137c:	movabs $0x7fffffffffffffc1,%rax
  651386:	cmp    %rax,%r15
  651389:	jae    651439 <valar_spiral_rs::poly::to_ntt_alloc+0xf9>
  65138f:	mov    %rsi,%rbx
  651392:	movq   $0x0,0x10(%rsp)
  65139b:	lea    0x10(%rsp),%rdi
  6513a0:	mov    $0x40,%esi
  6513a5:	mov    %r15,%rdx
  6513a8:	call   *0x864f2(%rip)        # 6d78a0 <posix_memalign@GLIBC_2.2.5>
  6513ae:	test   %eax,%eax
  6513b0:	setne  %al
  6513b3:	mov    0x10(%rsp),%r12
  6513b8:	test   %r12,%r12
  6513bb:	sete   %cl
  6513be:	or     %al,%cl
  6513c0:	je     6513c7 <valar_spiral_rs::poly::to_ntt_alloc+0x87>
  6513c2:	xor    %r12d,%r12d
  6513c5:	jmp    6513d5 <valar_spiral_rs::poly::to_ntt_alloc+0x95>
  6513c7:	mov    %r12,%rdi
  6513ca:	xor    %esi,%esi
  6513cc:	mov    %r15,%rdx
  6513cf:	call   *0x864c3(%rip)        # 6d7898 <memset@GLIBC_2.2.5>
  6513d5:	mov    %rbp,0x30(%rsp)
  6513da:	mov    0x8(%rsp),%rax
  6513df:	mov    %rax,0x38(%rsp)
  6513e4:	mov    %r14,0x40(%rsp)
  6513e9:	movq   $0x40,0x10(%rsp)
  6513f2:	mov    %r15,0x18(%rsp)
  6513f7:	mov    %r12,0x20(%rsp)
  6513fc:	mov    %r13,0x28(%rsp)
  651401:	lea    0x10(%rsp),%rdi
  651406:	mov    %rbx,%rsi
  651409:	call   651480 <valar_spiral_rs::poly::to_ntt>
  65140e:	vmovups 0x10(%rsp),%ymm0
  651414:	vmovups 0x28(%rsp),%ymm1
  65141a:	mov    (%rsp),%rax
  65141e:	vmovups %ymm1,0x18(%rax)
  651423:	vmovups %ymm0,(%rax)
  651427:	add    $0x48,%rsp
  65142b:	pop    %rbx
  65142c:	pop    %r12
  65142e:	pop    %r13
  651430:	pop    %r14
  651432:	pop    %r15
  651434:	pop    %rbp
  651435:	vzeroupper
  651438:	ret
  651439:	lea    -0x56b669(%rip),%rdi        # e5dd7 <serde_json::value::index::<impl core::ops::index::Index<I> for serde_json::value::Value>::index::NULL+0x26f>
  651440:	lea    0x84c29(%rip),%rcx        # 6d6070 <tokio::runtime::task::waker::WAKER_VTABLE+0x2ad8>
  651447:	lea    0x84492(%rip),%r8        # 6d58e0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2348>
  65144e:	lea    0x10(%rsp),%rdx
  651453:	mov    $0x2b,%esi
  651458:	call   293590 <core::result::unwrap_failed>
  65145d:	mov    %rax,%rbx
  651460:	mov    %r12,%rdi
  651463:	call   *0x863ef(%rip)        # 6d7858 <free@GLIBC_2.2.5>
  651469:	mov    %rbx,%rdi
  65146c:	call   6a51d0 <_Unwind_Resume@plt>
  651471:	int3
  651472:	int3
  651473:	int3
  651474:	int3
  651475:	int3
  651476:	int3
  651477:	int3
  651478:	int3
  651479:	int3
  65147a:	int3
  65147b:	int3
  65147c:	int3
  65147d:	int3
  65147e:	int3
  65147f:	int3

0000000000651480 <valar_spiral_rs::poly::to_ntt>:
  651480:	push   %rbp
  651481:	push   %r15
  651483:	push   %r14
  651485:	push   %r13
  651487:	push   %r12
  651489:	push   %rbx
  65148a:	sub    $0xb8,%rsp
  651491:	mov    0x28(%rdi),%rax
  651495:	mov    %rax,0x60(%rsp)
  65149a:	test   %rax,%rax
  65149d:	je     651c7b <valar_spiral_rs::poly::to_ntt+0x7fb>
  6514a3:	mov    0x30(%rdi),%r15
  6514a7:	test   %r15,%r15
  6514aa:	je     651c7b <valar_spiral_rs::poly::to_ntt+0x7fb>
  6514b0:	mov    0x20(%rdi),%r13
  6514b4:	mov    0x10(%rdi),%rbp
  6514b8:	mov    0x10(%rsi),%rax
  6514bc:	mov    %rax,0x48(%rsp)
  6514c1:	mov    0x20(%rsi),%rax
  6514c5:	mov    %rax,0x80(%rsp)
  6514cd:	mov    0x30(%rsi),%rax
  6514d1:	lea    0x0(,%r15,8),%rcx
  6514d9:	mov    %rcx,0x50(%rsp)
  6514de:	shl    $0x3,%rax
  6514e2:	mov    %rax,0x58(%rsp)
  6514e7:	vpbroadcastq -0x5d50d0(%rip),%ymm7        # 7c420 <GCC_except_table6770+0x41d0>
  6514f0:	xor    %eax,%eax
  6514f2:	xor    %ecx,%ecx
  6514f4:	xor    %edx,%edx
  6514f6:	mov    %r15,0x28(%rsp)
  6514fb:	mov    %r13,0x20(%rsp)
  651500:	mov    %rbp,0x18(%rsp)
  651505:	vmovdqu %ymm7,0x90(%rsp)
  65150e:	jmp    651534 <valar_spiral_rs::poly::to_ntt+0xb4>
  651510:	mov    0x70(%rsp),%rcx
  651515:	add    0x50(%rsp),%rcx
  65151a:	mov    0x78(%rsp),%rax
  65151f:	add    0x58(%rsp),%rax
  651524:	mov    0x68(%rsp),%rdx
  651529:	cmp    0x60(%rsp),%rdx
  65152e:	je     651c7b <valar_spiral_rs::poly::to_ntt+0x7fb>
  651534:	mov    %r15,%rsi
  651537:	imul   %rdx,%rsi
  65153b:	mov    %rsi,0x88(%rsp)
  651543:	inc    %rdx
  651546:	mov    %rdx,0x68(%rsp)
  65154b:	mov    %rax,0x78(%rsp)
  651550:	mov    %rax,%r14
  651553:	mov    %rcx,0x70(%rsp)
  651558:	mov    %rcx,%r12
  65155b:	xor    %r8d,%r8d
  65155e:	jmp    6515a6 <valar_spiral_rs::poly::to_ntt+0x126>
  651560:	lea    0x1(%r8),%rbx
  651564:	add    0x88(%rsp),%r8
  65156c:	imul   %rax,%r8
  651570:	lea    0x0(,%r8,8),%rsi
  651578:	add    %rbp,%rsi
  65157b:	mov    %r13,%rdi
  65157e:	mov    %rax,%rdx
  651581:	vzeroupper
  651584:	call   6503e0 <valar_spiral_rs::ntt::avx2::ntt_forward>
  651589:	vmovdqu 0x90(%rsp),%ymm7
  651592:	add    $0x8,%r12
  651596:	add    $0x8,%r14
  65159a:	mov    %rbx,%r8
  65159d:	cmp    %r15,%rbx
  6515a0:	je     651510 <valar_spiral_rs::poly::to_ntt+0x90>
  6515a6:	mov    0x30(%r13),%r9
  6515aa:	mov    0x40(%r13),%rcx
  6515ae:	mov    %rcx,%rax
  6515b1:	imul   %r9,%rax
  6515b5:	mov    %rcx,(%rsp)
  6515b9:	test   %rcx,%rcx
  6515bc:	sete   %cl
  6515bf:	test   %r9,%r9
  6515c2:	sete   %dl
  6515c5:	or     %cl,%dl
  6515c7:	jne    651560 <valar_spiral_rs::poly::to_ntt+0xe0>
  6515c9:	mov    0x80(%rsp),%rcx
  6515d1:	mov    0x30(%rcx),%rdi
  6515d5:	mov    0x68(%r13),%rcx
  6515d9:	mov    0xa8(%r13),%rsi
  6515e0:	cmp    %rax,%rdi
  6515e3:	mov    %rax,%r10
  6515e6:	cmovb  %rdi,%r10
  6515ea:	lea    -0x1(%r9),%rdx
  6515ee:	cmp    %rdx,%r10
  6515f1:	mov    %rdx,0x38(%rsp)
  6515f6:	cmovae %rdx,%r10
  6515fa:	inc    %r10
  6515fd:	cmp    $0x5,%r10
  651601:	mov    %r14,0x10(%rsp)
  651606:	mov    %r12,0x8(%rsp)
  65160b:	jae    651620 <valar_spiral_rs::poly::to_ntt+0x1a0>
  65160d:	xor    %r10d,%r10d
  651610:	jmp    651716 <valar_spiral_rs::poly::to_ntt+0x296>
  651615:	data16 cs nopw 0x0(%rax,%rax,1)
  651620:	mov    %r10d,%edx
  651623:	and    $0x3,%edx
  651626:	mov    $0x4,%r11d
  65162c:	cmove  %r11,%rdx
  651630:	sub    %rdx,%r10
  651633:	vmovq  %rsi,%xmm0
  651638:	vpbroadcastq %xmm0,%ymm0
  65163d:	mov    (%rsp),%r11
  651641:	imul   %r9,%r11
  651645:	imul   %r12,%r11
  651649:	add    %rbp,%r11
  65164c:	mov    %rdi,%rbx
  65164f:	imul   %r14,%rbx
  651653:	add    0x48(%rsp),%rbx
  651658:	xor    %r14d,%r14d
  65165b:	nopl   0x0(%rax,%rax,1)
  651660:	vmovdqu (%rbx,%r14,8),%ymm1
  651666:	vpextrq $0x1,%xmm1,%r15
  65166c:	mov    %rcx,%rdx
  65166f:	mulx   0x18(%rbx,%r14,8),%r12,%r12
  651676:	mulx   0x10(%rbx,%r14,8),%rbp,%rbp
  65167d:	vmovq  %xmm1,%r13
  651682:	mulx   %r13,%r13,%r13
  651687:	mulx   %r15,%rdx,%rdx
  65168c:	vmovq  %rbp,%xmm2
  651691:	vmovq  %r12,%xmm3
  651696:	vpunpcklqdq %xmm3,%xmm2,%xmm2
  65169a:	vmovq  %rdx,%xmm3
  65169f:	vmovq  %r13,%xmm4
  6516a4:	vpunpcklqdq %xmm3,%xmm4,%xmm3
  6516a8:	vinserti128 $0x1,%xmm2,%ymm3,%ymm2
  6516ae:	vpsrlq $0x20,%ymm0,%ymm3
  6516b3:	vpmuludq %ymm2,%ymm3,%ymm3
  6516b7:	vpsrlq $0x20,%ymm2,%ymm4
  6516bc:	vpmuludq %ymm4,%ymm0,%ymm4
  6516c0:	vpaddq %ymm3,%ymm4,%ymm3
  6516c4:	vpsllq $0x20,%ymm3,%ymm3
  6516c9:	vpmuludq %ymm2,%ymm0,%ymm2
  6516cd:	vpaddq %ymm3,%ymm2,%ymm2
  6516d1:	vpsubq %ymm2,%ymm1,%ymm1
  6516d5:	vpxor  %ymm7,%ymm1,%ymm2
  6516d9:	vpxor  %ymm7,%ymm0,%ymm3
  6516dd:	vpcmpgtq %ymm2,%ymm3,%ymm2
  6516e2:	vpandn %ymm0,%ymm2,%ymm2
  6516e6:	vpsubq %ymm2,%ymm1,%ymm1
  6516ea:	vmovdqu %ymm1,(%r11,%r14,8)
  6516f0:	add    $0x4,%r14
  6516f4:	cmp    %r14,%r10
  6516f7:	jne    651660 <valar_spiral_rs::poly::to_ntt+0x1e0>
  6516fd:	mov    0x28(%rsp),%r15
  651702:	mov    0x20(%rsp),%r13
  651707:	mov    0x18(%rsp),%rbp
  65170c:	mov    0x10(%rsp),%r14
  651711:	mov    0x8(%rsp),%r12
  651716:	mov    %rdi,%r11
  651719:	imul   %r14,%r11
  65171d:	add    0x48(%rsp),%r11
  651722:	mov    (%rsp),%rbx
  651726:	imul   %r9,%rbx
  65172a:	imul   %r12,%rbx
  65172e:	add    %rbp,%rbx
  651731:	data16 data16 data16 data16 data16 cs nopw 0x0(%rax,%rax,1)
  651740:	cmp    %r10,%rdi
  651743:	je     651c90 <valar_spiral_rs::poly::to_ntt+0x810>
  651749:	cmp    %r10,%rax
  65174c:	je     651ca2 <valar_spiral_rs::poly::to_ntt+0x822>
  651752:	mov    (%r11,%r10,8),%rdx
  651756:	mulx   %rcx,%r14,%r14
  65175b:	imul   %rsi,%r14
  65175f:	sub    %r14,%rdx
  651762:	cmp    %rsi,%rdx
  651765:	mov    $0x0,%r14d
  65176b:	cmovae %rsi,%r14
  65176f:	sub    %r14,%rdx
  651772:	mov    %rdx,(%rbx,%r10,8)
  651776:	inc    %r10
  651779:	cmp    %r10,%r9
  65177c:	jne    651740 <valar_spiral_rs::poly::to_ntt+0x2c0>
  65177e:	cmpq   $0x1,(%rsp)
  651783:	mov    0x10(%rsp),%r14
  651788:	mov    0x8(%rsp),%r12
  65178d:	je     651560 <valar_spiral_rs::poly::to_ntt+0xe0>
  651793:	mov    %rax,%rsi
  651796:	sub    %r9,%rsi
  651799:	mov    $0x0,%ecx
  65179e:	cmovb  %rcx,%rsi
  6517a2:	cmp    %rsi,%rdi
  6517a5:	cmovb  %rdi,%rsi
  6517a9:	mov    0x70(%r13),%rcx
  6517ad:	mov    0x38(%rsp),%rdx
  6517b2:	cmp    %rdx,%rsi
  6517b5:	cmovae %rdx,%rsi
  6517b9:	mov    0xb0(%r13),%r12
  6517c0:	inc    %rsi
  6517c3:	mov    $0x0,%ebx
  6517c8:	cmp    $0x5,%rsi
  6517cc:	jb     6518b6 <valar_spiral_rs::poly::to_ntt+0x436>
  6517d2:	mov    %esi,%edx
  6517d4:	and    $0x3,%edx
  6517d7:	mov    $0x4,%r10d
  6517dd:	cmove  %r10,%rdx
  6517e1:	sub    %rdx,%rsi
  6517e4:	vmovq  %r12,%xmm0
  6517e9:	vpbroadcastq %xmm0,%ymm0
  6517ee:	mov    (%rsp),%r10
  6517f2:	imul   0x8(%rsp),%r10
  6517f8:	add    $0x8,%r10
  6517fc:	imul   %r9,%r10
  651800:	add    %rbp,%r10
  651803:	vpsrlq $0x20,%ymm0,%ymm1
  651808:	vpxor  %ymm7,%ymm0,%ymm2
  65180c:	xor    %ebx,%ebx
  65180e:	xchg   %ax,%ax
  651810:	vmovdqu (%r11,%rbx,8),%ymm3
  651816:	vpextrq $0x1,%xmm3,%r14
  65181c:	mov    %rcx,%rdx
  65181f:	mulx   0x18(%r11,%rbx,8),%r15,%r15
  651826:	vmovq  %xmm3,%r13
  65182b:	mulx   0x10(%r11,%rbx,8),%rbp,%rbp
  651832:	mulx   %r13,%r13,%r13
  651837:	mulx   %r14,%rdx,%rdx
  65183c:	vmovq  %rbp,%xmm4
  651841:	vmovq  %r15,%xmm5
  651846:	vpunpcklqdq %xmm5,%xmm4,%xmm4
  65184a:	vmovq  %rdx,%xmm5
  65184f:	vmovq  %r13,%xmm6
  651854:	vpunpcklqdq %xmm5,%xmm6,%xmm5
  651858:	vinserti128 $0x1,%xmm4,%ymm5,%ymm4
  65185e:	vpmuludq %ymm4,%ymm1,%ymm5
  651862:	vpsrlq $0x20,%ymm4,%ymm6
  651867:	vpmuludq %ymm6,%ymm0,%ymm6
  65186b:	vpaddq %ymm5,%ymm6,%ymm5
  65186f:	vpsllq $0x20,%ymm5,%ymm5
  651874:	vpmuludq %ymm4,%ymm0,%ymm4
  651878:	vpaddq %ymm5,%ymm4,%ymm4
  65187c:	vpsubq %ymm4,%ymm3,%ymm3
  651880:	vpxor  %ymm7,%ymm3,%ymm4
  651884:	vpcmpgtq %ymm4,%ymm2,%ymm4
  651889:	vpandn %ymm0,%ymm4,%ymm4
  65188d:	vpsubq %ymm4,%ymm3,%ymm3
  651891:	vmovdqu %ymm3,(%r10,%rbx,8)
  651897:	add    $0x4,%rbx
  65189b:	cmp    %rbx,%rsi
  65189e:	jne    651810 <valar_spiral_rs::poly::to_ntt+0x390>
  6518a4:	mov    %rsi,%rbx
  6518a7:	mov    0x28(%rsp),%r15
  6518ac:	mov    0x20(%rsp),%r13
  6518b1:	mov    0x18(%rsp),%rbp
  6518b6:	mov    (%rsp),%r10
  6518ba:	imul   0x8(%rsp),%r10
  6518c0:	lea    0x8(%r10),%r14
  6518c4:	imul   %r9,%r14
  6518c8:	add    %rbp,%r14
  6518cb:	nopl   0x0(%rax,%rax,1)
  6518d0:	cmp    %rbx,%rdi
  6518d3:	je     651c90 <valar_spiral_rs::poly::to_ntt+0x810>
  6518d9:	lea    (%r9,%rbx,1),%rsi
  6518dd:	cmp    %rax,%rsi
  6518e0:	jae    651ca5 <valar_spiral_rs::poly::to_ntt+0x825>
  6518e6:	mov    (%r11,%rbx,8),%rdx
  6518ea:	mulx   %rcx,%rsi,%rsi
  6518ef:	imul   %r12,%rsi
  6518f3:	sub    %rsi,%rdx
  6518f6:	cmp    %r12,%rdx
  6518f9:	mov    $0x0,%esi
  6518fe:	cmovae %r12,%rsi
  651902:	sub    %rsi,%rdx
  651905:	mov    %rdx,(%r14,%rbx,8)
  651909:	inc    %rbx
  65190c:	cmp    %rbx,%r9
  65190f:	jne    6518d0 <valar_spiral_rs::poly::to_ntt+0x450>
  651911:	cmpq   $0x2,(%rsp)
  651916:	mov    0x10(%rsp),%r14
  65191b:	mov    0x8(%rsp),%r12
  651920:	je     651560 <valar_spiral_rs::poly::to_ntt+0xe0>
  651926:	lea    (%r9,%r9,1),%rcx
  65192a:	mov    %rax,%r14
  65192d:	mov    %rcx,0x40(%rsp)
  651932:	sub    %rcx,%r14
  651935:	mov    $0x0,%ecx
  65193a:	cmovb  %rcx,%r14
  65193e:	cmp    %r14,%rdi
  651941:	cmovb  %rdi,%r14
  651945:	mov    0x78(%r13),%rcx
  651949:	mov    0x38(%rsp),%rdx
  65194e:	cmp    %rdx,%r14
  651951:	cmovae %rdx,%r14
  651955:	mov    0xb8(%r13),%r12
  65195c:	inc    %r14
  65195f:	mov    $0x0,%ebx
  651964:	cmp    $0x5,%r14
  651968:	jb     651a5b <valar_spiral_rs::poly::to_ntt+0x5db>
  65196e:	mov    %r14d,%edx
  651971:	and    $0x3,%edx
  651974:	mov    $0x4,%esi
  651979:	cmove  %rsi,%rdx
  65197d:	sub    %rdx,%r14
  651980:	vmovq  %r12,%xmm0
  651985:	vpbroadcastq %xmm0,%ymm0
  65198a:	mov    %r10,0x30(%rsp)
  65198f:	lea    0x10(%r10),%rbx
  651993:	imul   %r9,%rbx
  651997:	add    %rbp,%rbx
  65199a:	vpsrlq $0x20,%ymm0,%ymm1
  65199f:	vpxor  %ymm7,%ymm0,%ymm2
  6519a3:	xor    %r15d,%r15d
  6519a6:	cs nopw 0x0(%rax,%rax,1)
  6519b0:	vmovdqu (%r11,%r15,8),%ymm3
  6519b6:	vpextrq $0x1,%xmm3,%r13
  6519bc:	mov    %rcx,%rdx
  6519bf:	mulx   0x18(%r11,%r15,8),%rbp,%rbp
  6519c6:	vmovq  %xmm3,%r10
  6519cb:	mulx   0x10(%r11,%r15,8),%rsi,%rsi
  6519d2:	mulx   %r10,%r10,%r10
  6519d7:	mulx   %r13,%rdx,%rdx
  6519dc:	vmovq  %rsi,%xmm4
  6519e1:	vmovq  %rbp,%xmm5
  6519e6:	vpunpcklqdq %xmm5,%xmm4,%xmm4
  6519ea:	vmovq  %rdx,%xmm5
  6519ef:	vmovq  %r10,%xmm6
  6519f4:	vpunpcklqdq %xmm5,%xmm6,%xmm5
  6519f8:	vinserti128 $0x1,%xmm4,%ymm5,%ymm4
  6519fe:	vpmuludq %ymm4,%ymm1,%ymm5
  651a02:	vpsrlq $0x20,%ymm4,%ymm6
  651a07:	vpmuludq %ymm6,%ymm0,%ymm6
  651a0b:	vpaddq %ymm5,%ymm6,%ymm5
  651a0f:	vpsllq $0x20,%ymm5,%ymm5
  651a14:	vpmuludq %ymm4,%ymm0,%ymm4
  651a18:	vpaddq %ymm5,%ymm4,%ymm4
  651a1c:	vpsubq %ymm4,%ymm3,%ymm3
  651a20:	vpxor  %ymm7,%ymm3,%ymm4
  651a24:	vpcmpgtq %ymm4,%ymm2,%ymm4
  651a29:	vpandn %ymm0,%ymm4,%ymm4
  651a2d:	vpsubq %ymm4,%ymm3,%ymm3
  651a31:	vmovdqu %ymm3,(%rbx,%r15,8)
  651a37:	add    $0x4,%r15
  651a3b:	cmp    %r15,%r14
  651a3e:	jne    6519b0 <valar_spiral_rs::poly::to_ntt+0x530>
  651a44:	mov    %r14,%rbx
  651a47:	mov    0x28(%rsp),%r15
  651a4c:	mov    0x20(%rsp),%r13
  651a51:	mov    0x18(%rsp),%rbp
  651a56:	mov    0x30(%rsp),%r10
  651a5b:	lea    0x10(%r10),%r14
  651a5f:	imul   %r9,%r14
  651a63:	add    %rbp,%r14
  651a66:	cs nopw 0x0(%rax,%rax,1)
  651a70:	cmp    %rbx,%rdi
  651a73:	je     651c90 <valar_spiral_rs::poly::to_ntt+0x810>
  651a79:	mov    0x40(%rsp),%rdx
  651a7e:	add    %rbx,%rdx
  651a81:	cmp    %rax,%rdx
  651a84:	jae    651cba <valar_spiral_rs::poly::to_ntt+0x83a>
  651a8a:	mov    (%r11,%rbx,8),%rdx
  651a8e:	mulx   %rcx,%rsi,%rsi
  651a93:	imul   %r12,%rsi
  651a97:	sub    %rsi,%rdx
  651a9a:	cmp    %r12,%rdx
  651a9d:	mov    $0x0,%esi
  651aa2:	cmovae %r12,%rsi
  651aa6:	sub    %rsi,%rdx
  651aa9:	mov    %rdx,(%r14,%rbx,8)
  651aad:	inc    %rbx
  651ab0:	cmp    %rbx,%r9
  651ab3:	jne    651a70 <valar_spiral_rs::poly::to_ntt+0x5f0>
  651ab5:	cmpq   $0x3,(%rsp)
  651aba:	mov    0x10(%rsp),%r14
  651abf:	mov    0x8(%rsp),%r12
  651ac4:	je     651560 <valar_spiral_rs::poly::to_ntt+0xe0>
  651aca:	lea    (%r9,%r9,2),%r12
  651ace:	mov    %rax,%rsi
  651ad1:	sub    %r12,%rsi
  651ad4:	mov    $0x0,%ecx
  651ad9:	cmovb  %rcx,%rsi
  651add:	cmp    %rsi,%rdi
  651ae0:	cmovb  %rdi,%rsi
  651ae4:	mov    0x80(%r13),%rcx
  651aeb:	mov    0x38(%rsp),%rdx
  651af0:	cmp    %rdx,%rsi
  651af3:	cmovae %rdx,%rsi
  651af7:	mov    0xc0(%r13),%r14
  651afe:	inc    %rsi
  651b01:	mov    $0x0,%ebx
  651b06:	cmp    $0x5,%rsi
  651b0a:	jb     651c00 <valar_spiral_rs::poly::to_ntt+0x780>
  651b10:	mov    %r12,0x40(%rsp)
  651b15:	mov    %esi,%edx
  651b17:	and    $0x3,%edx
  651b1a:	mov    %r10,%rbx
  651b1d:	mov    $0x4,%r10d
  651b23:	cmove  %r10,%rdx
  651b27:	sub    %rdx,%rsi
  651b2a:	vmovq  %r14,%xmm0
  651b2f:	vpbroadcastq %xmm0,%ymm0
  651b34:	mov    %rbx,0x30(%rsp)
  651b39:	add    $0x18,%rbx
  651b3d:	imul   %r9,%rbx
  651b41:	add    %rbp,%rbx
  651b44:	vpsrlq $0x20,%ymm0,%ymm1
  651b49:	vpxor  %ymm7,%ymm0,%ymm2
  651b4d:	xor    %r15d,%r15d
  651b50:	vmovdqu (%r11,%r15,8),%ymm3
  651b56:	vpextrq $0x1,%xmm3,%r10
  651b5c:	mov    %rcx,%rdx
  651b5f:	mulx   0x18(%r11,%r15,8),%r13,%r13
  651b66:	vmovq  %xmm3,%rbp
  651b6b:	mulx   0x10(%r11,%r15,8),%r12,%r12
  651b72:	mulx   %rbp,%rbp,%rbp
  651b77:	mulx   %r10,%rdx,%rdx
  651b7c:	vmovq  %r12,%xmm4
  651b81:	vmovq  %r13,%xmm5
  651b86:	vpunpcklqdq %xmm5,%xmm4,%xmm4
  651b8a:	vmovq  %rdx,%xmm5
  651b8f:	vmovq  %rbp,%xmm6
  651b94:	vpunpcklqdq %xmm5,%xmm6,%xmm5
  651b98:	vinserti128 $0x1,%xmm4,%ymm5,%ymm4
  651b9e:	vpmuludq %ymm4,%ymm1,%ymm5
  651ba2:	vpsrlq $0x20,%ymm4,%ymm6
  651ba7:	vpmuludq %ymm6,%ymm0,%ymm6
  651bab:	vpaddq %ymm5,%ymm6,%ymm5
  651baf:	vpsllq $0x20,%ymm5,%ymm5
  651bb4:	vpmuludq %ymm4,%ymm0,%ymm4
  651bb8:	vpaddq %ymm5,%ymm4,%ymm4
  651bbc:	vpsubq %ymm4,%ymm3,%ymm3
  651bc0:	vpxor  %ymm7,%ymm3,%ymm4
  651bc4:	vpcmpgtq %ymm4,%ymm2,%ymm4
  651bc9:	vpandn %ymm0,%ymm4,%ymm4
  651bcd:	vpsubq %ymm4,%ymm3,%ymm3
  651bd1:	vmovdqu %ymm3,(%rbx,%r15,8)
  651bd7:	add    $0x4,%r15
  651bdb:	cmp    %r15,%rsi
  651bde:	jne    651b50 <valar_spiral_rs::poly::to_ntt+0x6d0>
  651be4:	mov    %rsi,%rbx
  651be7:	mov    0x28(%rsp),%r15
  651bec:	mov    0x20(%rsp),%r13
  651bf1:	mov    0x18(%rsp),%rbp
  651bf6:	mov    0x30(%rsp),%r10
  651bfb:	mov    0x40(%rsp),%r12
  651c00:	add    $0x18,%r10
  651c04:	imul   %r9,%r10
  651c08:	add    %rbp,%r10
  651c0b:	nopl   0x0(%rax,%rax,1)
  651c10:	cmp    %rbx,%rdi
  651c13:	je     651c90 <valar_spiral_rs::poly::to_ntt+0x810>
  651c15:	lea    (%r12,%rbx,1),%rsi
  651c19:	cmp    %rax,%rsi
  651c1c:	jae    651ca5 <valar_spiral_rs::poly::to_ntt+0x825>
  651c22:	mov    (%r11,%rbx,8),%rdx
  651c26:	mulx   %rcx,%rsi,%rsi
  651c2b:	imul   %r14,%rsi
  651c2f:	sub    %rsi,%rdx
  651c32:	cmp    %r14,%rdx
  651c35:	mov    $0x0,%esi
  651c3a:	cmovae %r14,%rsi
  651c3e:	sub    %rsi,%rdx
  651c41:	mov    %rdx,(%r10,%rbx,8)
  651c45:	inc    %rbx
  651c48:	cmp    %rbx,%r9
  651c4b:	jne    651c10 <valar_spiral_rs::poly::to_ntt+0x790>
  651c4d:	cmpq   $0x4,(%rsp)
  651c52:	mov    0x10(%rsp),%r14
  651c57:	mov    0x8(%rsp),%r12
  651c5c:	je     651560 <valar_spiral_rs::poly::to_ntt+0xe0>
  651c62:	lea    0x8425f(%rip),%rdx        # 6d5ec8 <tokio::runtime::task::waker::WAKER_VTABLE+0x2930>
  651c69:	mov    $0x4,%edi
  651c6e:	mov    $0x4,%esi
  651c73:	vzeroupper
  651c76:	call   28beb0 <core::panicking::panic_bounds_check>
  651c7b:	add    $0xb8,%rsp
  651c82:	pop    %rbx
  651c83:	pop    %r12
  651c85:	pop    %r13
  651c87:	pop    %r14
  651c89:	pop    %r15
  651c8b:	pop    %rbp
  651c8c:	vzeroupper
  651c8f:	ret
  651c90:	lea    0x83fd9(%rip),%rdx        # 6d5c70 <tokio::runtime::task::waker::WAKER_VTABLE+0x26d8>
  651c97:	mov    %rdi,%rsi
  651c9a:	vzeroupper
  651c9d:	call   28beb0 <core::panicking::panic_bounds_check>
  651ca2:	mov    %rax,%rsi
  651ca5:	lea    0x83fdc(%rip),%rdx        # 6d5c88 <tokio::runtime::task::waker::WAKER_VTABLE+0x26f0>
  651cac:	mov    %rsi,%rdi
  651caf:	mov    %rax,%rsi
  651cb2:	vzeroupper
  651cb5:	call   28beb0 <core::panicking::panic_bounds_check>
  651cba:	lea    (%rbx,%r9,2),%rsi
  651cbe:	lea    0x83fc3(%rip),%rdx        # 6d5c88 <tokio::runtime::task::waker::WAKER_VTABLE+0x26f0>
  651cc5:	mov    %rsi,%rdi
  651cc8:	mov    %rax,%rsi
  651ccb:	vzeroupper
  651cce:	call   28beb0 <core::panicking::panic_bounds_check>
  651cd3:	int3
  651cd4:	int3
  651cd5:	int3
  651cd6:	int3
  651cd7:	int3
  651cd8:	int3
  651cd9:	int3
  651cda:	int3
  651cdb:	int3
  651cdc:	int3
  651cdd:	int3
  651cde:	int3
  651cdf:	int3

0000000000651ce0 <valar_spiral_rs::poly::from_ntt_alloc>:
  651ce0:	push   %rbp
  651ce1:	push   %r15
  651ce3:	push   %r14
  651ce5:	push   %r13
  651ce7:	push   %r12
  651ce9:	push   %rbx
  651cea:	sub    $0x48,%rsp
  651cee:	mov    0x20(%rsi),%rbp
  651cf2:	mov    0x28(%rsi),%rax
  651cf6:	mov    0x30(%rsi),%rbx
  651cfa:	mov    %rbx,%r13
  651cfd:	mov    %rax,0x8(%rsp)
  651d02:	imul   %rax,%r13
  651d06:	imul   0x30(%rbp),%r13
  651d0b:	mov    %rdi,(%rsp)
  651d0f:	lea    0x0(,%r13,8),%r15
  651d17:	movabs $0x7fffffffffffffc1,%rax
  651d21:	cmp    %rax,%r15
  651d24:	jae    651dd4 <valar_spiral_rs::poly::from_ntt_alloc+0xf4>
  651d2a:	mov    %rsi,%r14
  651d2d:	movq   $0x0,0x10(%rsp)
  651d36:	lea    0x10(%rsp),%rdi
  651d3b:	mov    $0x40,%esi
  651d40:	mov    %r15,%rdx
  651d43:	call   *0x85b57(%rip)        # 6d78a0 <posix_memalign@GLIBC_2.2.5>
  651d49:	test   %eax,%eax
  651d4b:	setne  %al
  651d4e:	mov    0x10(%rsp),%r12
  651d53:	test   %r12,%r12
  651d56:	sete   %cl
  651d59:	or     %al,%cl
  651d5b:	je     651d62 <valar_spiral_rs::poly::from_ntt_alloc+0x82>
  651d5d:	xor    %r12d,%r12d
  651d60:	jmp    651d70 <valar_spiral_rs::poly::from_ntt_alloc+0x90>
  651d62:	mov    %r12,%rdi
  651d65:	xor    %esi,%esi
  651d67:	mov    %r15,%rdx
  651d6a:	call   *0x85b28(%rip)        # 6d7898 <memset@GLIBC_2.2.5>
  651d70:	mov    %rbp,0x30(%rsp)
  651d75:	mov    0x8(%rsp),%rax
  651d7a:	mov    %rax,0x38(%rsp)
  651d7f:	mov    %rbx,0x40(%rsp)
  651d84:	movq   $0x40,0x10(%rsp)
  651d8d:	mov    %r15,0x18(%rsp)
  651d92:	mov    %r12,0x20(%rsp)
  651d97:	mov    %r13,0x28(%rsp)
  651d9c:	lea    0x10(%rsp),%rdi
  651da1:	mov    %r14,%rsi
  651da4:	call   651e10 <valar_spiral_rs::poly::from_ntt>
  651da9:	vmovups 0x10(%rsp),%ymm0
  651daf:	vmovups 0x28(%rsp),%ymm1
  651db5:	mov    (%rsp),%rax
  651db9:	vmovups %ymm1,0x18(%rax)
  651dbe:	vmovups %ymm0,(%rax)
  651dc2:	add    $0x48,%rsp
  651dc6:	pop    %rbx
  651dc7:	pop    %r12
  651dc9:	pop    %r13
  651dcb:	pop    %r14
  651dcd:	pop    %r15
  651dcf:	pop    %rbp
  651dd0:	vzeroupper
  651dd3:	ret
  651dd4:	lea    -0x56c004(%rip),%rdi        # e5dd7 <serde_json::value::index::<impl core::ops::index::Index<I> for serde_json::value::Value>::index::NULL+0x26f>
  651ddb:	lea    0x8428e(%rip),%rcx        # 6d6070 <tokio::runtime::task::waker::WAKER_VTABLE+0x2ad8>
  651de2:	lea    0x83af7(%rip),%r8        # 6d58e0 <tokio::runtime::task::waker::WAKER_VTABLE+0x2348>
  651de9:	lea    0x10(%rsp),%rdx
  651dee:	mov    $0x2b,%esi
  651df3:	call   293590 <core::result::unwrap_failed>
  651df8:	mov    %rax,%rbx
  651dfb:	mov    %r12,%rdi
  651dfe:	call   *0x85a54(%rip)        # 6d7858 <free@GLIBC_2.2.5>
  651e04:	mov    %rbx,%rdi
  651e07:	call   6a51d0 <_Unwind_Resume@plt>
  651e0c:	int3
  651e0d:	int3
  651e0e:	int3
  651e0f:	int3
